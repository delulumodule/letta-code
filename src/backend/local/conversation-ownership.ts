import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  mkdir,
  readdir,
  readFile,
  rename,
  unlink,
  writeFile,
} from "node:fs/promises";
import { join } from "node:path";
import { APIUserAbortError } from "@letta-ai/letta-client/core/error";
import { Stream } from "@letta-ai/letta-client/core/streaming";
import type { LettaStreamingResponse } from "@letta-ai/letta-client/resources/agents/messages";
import type {
  ConversationMessageCreateBody,
  ConversationMessageStreamBody,
} from "@/backend/backend";
import { tryAcquireFileLock, withFileLock } from "@/utils/file-lock";
import { localConversationKey } from "./local-conversation-key";

type TurnBody = ConversationMessageCreateBody | ConversationMessageStreamBody;
type ExecuteTurn = (
  body: TurnBody,
  signal: AbortSignal,
  onInputPersisted: () => Promise<void>,
) => Promise<Stream<LettaStreamingResponse>>;
type InboxMessage = { role: "user"; content: string; otid: string };
type Owner = {
  release: () => Promise<void>;
  busy: boolean;
  waitingForTools: boolean;
  runId?: string;
  controller?: AbortController;
  abort?: () => void;
  releasing?: Promise<void>;
};

/**
 * Owns the entire local provider/tool loop, including the gaps between approval streams.
 * Admission and final inbox checks share a short lock so a sender cannot enqueue behind
 * an owner that has already decided to exit. Live owners are never evicted by elapsed time.
 */
export class LocalConversationOwnership {
  private readonly owners = new Map<string, Owner>();

  constructor(private readonly storageDir?: string) {}

  async execute(
    conversationId: string,
    agentId: string,
    body: TurnBody,
    execute: ExecuteTurn,
    getInputIds: () => Iterable<string>,
    signal?: AbortSignal,
  ): Promise<Stream<LettaStreamingResponse>> {
    if (signal?.aborted) {
      throw new APIUserAbortError();
    }
    const key = localConversationKey(conversationId, agentId);
    const owner = await this.admit(key, signal);
    if (this.storageDir && owner.waitingForTools && !hasApprovalInput(body)) {
      owner.busy = false;
      throw this.busyError(key);
    }
    const ownership = this;
    const controller = new AbortController();
    const abort = () => {
      controller.abort();
    };
    owner.abort = abort;
    let stream: Stream<LettaStreamingResponse>;
    signal?.addEventListener("abort", abort, { once: true });
    controller.signal.addEventListener(
      "abort",
      () => {
        if (owner.controller === controller) {
          stream.controller.abort();
          void ownership.release(key, owner);
        }
      },
      { once: true },
    );
    if (signal?.aborted) {
      abort();
    }
    const start = async (input: TurnBody, queued: InboxMessage[]) => {
      owner.controller = undefined;
      const next = await execute(input, controller.signal, () =>
        this.acknowledge(key, queued),
      );
      if (controller.signal.aborted) {
        next.controller.abort();
        throw new APIUserAbortError();
      }
      owner.controller = controller;
      return next;
    };
    try {
      const queued = owner.waitingForTools
        ? await this.pending(key, getInputIds)
        : [];
      const input =
        queued.length > 0 && body && "messages" in body
          ? { ...body, messages: [...(body.messages ?? []), ...queued] }
          : body;
      stream = await start(input, queued);
    } catch (error) {
      signal?.removeEventListener("abort", abort);
      await this.release(key, owner);
      throw error;
    }
    return new Stream(async function* () {
      let retained = false;
      try {
        for (;;) {
          let terminal: LettaStreamingResponse | undefined;
          for await (const chunk of stream) {
            if (controller.signal.aborted) {
              throw new APIUserAbortError();
            }
            if (chunk.message_type === "stop_reason") {
              terminal = chunk;
            } else {
              yield chunk;
            }
          }
          const reason =
            terminal && "stop_reason" in terminal
              ? terminal.stop_reason
              : undefined;
          if (reason === "requires_approval") {
            owner.waitingForTools = true;
            owner.busy = false;
            retained = true;
          } else if (reason === "end_turn") {
            const queued = await ownership.nextOrRelease(
              key,
              owner,
              getInputIds,
            );
            if (queued.length > 0) {
              stream = await start({ ...body, messages: queued }, queued);
              continue;
            }
          }
          if (terminal) {
            yield terminal;
          }
          break;
        }
      } finally {
        signal?.removeEventListener("abort", abort);
        if (!retained) {
          stream.controller.abort();
          await ownership.release(key, owner);
        }
      }
    }, controller);
  }

  /** Bind provider-run identity to the longer-lived conversation/tool-loop owner. */
  recordRun(conversationId: string, agentId: string, runId: string): void {
    const owner = this.owners.get(
      localConversationKey(conversationId, agentId),
    );
    assert(
      owner?.busy,
      "A local run must have a conversation owner before it starts",
    );
    owner.runId = runId;
  }

  currentRunId(conversationId: string, agentId: string): string | undefined {
    return this.owners.get(localConversationKey(conversationId, agentId))
      ?.runId;
  }

  /** Queue to a live owner without waiting for its tools; false means launch a new CLI. */
  async enqueue(
    conversationId: string,
    agentId: string,
    content: string,
    otid: string,
  ): Promise<boolean> {
    if (!this.storageDir) {
      return false;
    }
    const key = localConversationKey(conversationId, agentId);
    return this.admission(key, async () => {
      const release = await this.lock(key, false);
      if (release) {
        await release();
        return false;
      }
      const inbox = join(await this.directory(key), "inbox");
      await mkdir(inbox, { recursive: true });
      if (!/^[a-zA-Z0-9-]+$/.test(otid)) {
        throw new Error("Invalid local message id");
      }
      const temporaryPath = join(inbox, `${randomUUID()}.tmp`);
      await writeFile(
        temporaryPath,
        JSON.stringify({
          role: "user",
          content,
          otid,
          sequence: process.hrtime.bigint().toString(),
        }),
        { flag: "wx" },
      );
      await rename(temporaryPath, join(inbox, `${otid}.json`));
      return true;
    });
  }

  /** Recovery and cancellation must not infer that another owner's tools were interrupted. */
  async assertAvailable(
    conversationId: string,
    agentId: string,
  ): Promise<void> {
    const key = localConversationKey(conversationId, agentId);
    await this.admission(key, async () => {
      const owner = this.owners.get(key);
      if (owner?.releasing) {
        await owner.releasing;
      } else if (owner?.waitingForTools) {
        throw this.busyError(key);
      } else if (owner) {
        return;
      }
      const release = await this.lock(key, false);
      if (!release) {
        throw this.busyError(key);
      }
      await release();
    });
  }

  /** Detached sends wait before reading approvals, rather than competing with a live loop. */
  async wait(
    conversationId: string,
    agentId: string,
    signal?: AbortSignal,
  ): Promise<void> {
    if (signal?.aborted) {
      throw new APIUserAbortError();
    }
    const key = localConversationKey(conversationId, agentId);
    if (this.owners.has(key)) {
      return;
    }
    const release = await this.lock(key, true, signal);
    if (!release) {
      throw this.busyError(key);
    }
    if (signal?.aborted) {
      await release();
      throw new APIUserAbortError();
    }
    this.owners.set(key, { release, busy: false, waitingForTools: false });
  }

  /** Serialize transcript maintenance with provider/tool owners in other processes. */
  async mutate<T>(
    conversationId: string,
    agentId: string,
    mutation: () => Promise<T>,
  ): Promise<T> {
    const key = localConversationKey(conversationId, agentId);
    const existing = this.owners.get(key);
    const owner = await this.admit(key);
    try {
      return await mutation();
    } finally {
      if (existing === owner && !owner.releasing) {
        owner.busy = false;
      } else {
        await this.release(key, owner);
      }
    }
  }

  /** Release an idle approval lease after its owning harness cancels the tool loop. */
  async cancel<T>(
    conversationId: string,
    agentId: string,
    cancel: () => Promise<T>,
  ): Promise<T> {
    const key = localConversationKey(conversationId, agentId);
    let existing = this.owners.get(key);
    if (existing?.releasing) {
      await existing.releasing;
      existing = undefined;
    }
    const owner = existing ?? (await this.admit(key));
    try {
      return await cancel();
    } finally {
      owner.abort?.();
      if (!existing || !owner.busy || owner.controller) {
        await this.release(key, owner);
      }
    }
  }

  private async admit(key: string, signal?: AbortSignal): Promise<Owner> {
    return this.admission(key, async () => {
      let existing = this.owners.get(key);
      if (existing?.releasing) {
        await existing.releasing;
        existing = undefined;
      }
      if (existing?.busy) {
        throw this.busyError(key);
      }
      if (existing) {
        existing.busy = true;
        existing.controller = undefined;
        return existing;
      }
      const release = this.storageDir
        ? await this.lock(key, false, signal)
        : async () => {};
      if (!release) {
        throw this.busyError(key);
      }
      const owner = { release, busy: true, waitingForTools: false };
      this.owners.set(key, owner);
      return owner;
    });
  }

  private async nextOrRelease(
    key: string,
    owner: Owner,
    getInputIds: () => Iterable<string>,
  ): Promise<InboxMessage[]> {
    return this.admission(key, async () => {
      const pending = await this.pending(key, getInputIds);
      if (pending.length === 0) {
        await this.release(key, owner);
      }
      return pending;
    });
  }

  private async pending(
    key: string,
    getInputIds: () => Iterable<string>,
  ): Promise<InboxMessage[]> {
    const messages = await this.readInbox(key);
    if (messages.length === 0) {
      return [];
    }
    const persistedIds = new Set(getInputIds());
    const pending = messages.filter(
      (message) => !persistedIds.has(message.otid),
    );
    await this.acknowledge(
      key,
      messages.filter((message) => persistedIds.has(message.otid)),
    );
    return pending;
  }

  private async readInbox(key: string): Promise<InboxMessage[]> {
    if (!this.storageDir) {
      return [];
    }
    const inbox = join(await this.directory(key), "inbox");
    let files: string[];
    try {
      files = await readdir(inbox);
    } catch (error) {
      if (isMissing(error)) {
        return [];
      }
      throw error;
    }
    const messages: Array<InboxMessage & { sequence: bigint }> = [];
    for (const file of files) {
      if (!file.endsWith(".json")) {
        continue;
      }
      const value: unknown = JSON.parse(
        await readFile(join(inbox, file), "utf8"),
      );
      if (
        typeof value !== "object" ||
        value === null ||
        !("content" in value) ||
        typeof value.content !== "string" ||
        !("otid" in value) ||
        typeof value.otid !== "string" ||
        file !== `${value.otid}.json` ||
        !("sequence" in value) ||
        typeof value.sequence !== "string" ||
        !/^\d+$/.test(value.sequence)
      ) {
        throw new Error(`Invalid local inbox message: ${file}`);
      }
      messages.push({
        role: "user",
        content: value.content,
        otid: value.otid,
        sequence: BigInt(value.sequence),
      });
    }
    return messages
      .toSorted((first, second) =>
        first.sequence < second.sequence
          ? -1
          : first.sequence > second.sequence
            ? 1
            : 0,
      )
      .map(({ sequence: _sequence, ...message }) => message);
  }

  private async acknowledge(
    key: string,
    messages: InboxMessage[],
  ): Promise<void> {
    if (!this.storageDir || messages.length === 0) {
      return;
    }
    const inbox = join(await this.directory(key), "inbox");
    for (const message of messages) {
      await unlink(join(inbox, `${message.otid}.json`));
    }
  }

  private async admission<T>(
    key: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    if (!this.storageDir) {
      return operation();
    }
    return withFileLock(
      join(await this.directory(key), "admission.lock"),
      operation,
      { reapOnlyDeadOwner: true, timeoutMs: Infinity },
    );
  }

  private async lock(
    key: string,
    wait: boolean,
    signal?: AbortSignal,
  ): Promise<(() => Promise<void>) | null> {
    if (!this.storageDir) {
      return async () => {};
    }
    return tryAcquireFileLock(join(await this.directory(key), "owner.lock"), {
      reapOnlyDeadOwner: true,
      timeoutMs: wait ? Infinity : 0,
      signal,
    });
  }

  private async directory(key: string): Promise<string> {
    if (!this.storageDir) {
      throw new Error("Conversation ownership requires a storage directory");
    }
    const directory = join(
      this.storageDir,
      "ownership",
      Buffer.from(key).toString("base64url"),
    );
    await mkdir(directory, { recursive: true });
    return directory;
  }

  private async release(key: string, owner: Owner): Promise<void> {
    if (owner.releasing) {
      return owner.releasing;
    }
    if (this.owners.get(key) !== owner) {
      return;
    }
    owner.releasing = (async () => {
      await owner.release();
      this.owners.delete(key);
    })();
    return owner.releasing;
  }

  private busyError(key: string): Error {
    return new Error(
      `Conversation ${key} already has an active run or tool loop in another owner`,
    );
  }
}

function isMissing(error: unknown): boolean {
  return error instanceof Error && "code" in error && error.code === "ENOENT";
}

function hasApprovalInput(body: TurnBody): boolean {
  return (
    body !== undefined &&
    "messages" in body &&
    Array.isArray(body.messages) &&
    body.messages.some(
      (message) => "type" in message && message.type === "approval",
    )
  );
}
