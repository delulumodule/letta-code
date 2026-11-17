import { afterEach, expect, test } from "bun:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import {
  closeSync,
  mkdtempSync,
  openSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Stream } from "@letta-ai/letta-client/core/streaming";
import type { LettaStreamingResponse } from "@letta-ai/letta-client/resources/agents/messages";
import { HeadlessBackend } from "@/backend/dev/fake-headless-backend";
import {
  createAssistantMessageStream,
  DeterministicToolCallExecutor,
} from "@/backend/dev/headless-turn-executor";
import { LocalBackend } from "@/backend/local/local-backend";
import { LocalStore } from "@/backend/local/local-store";
import { runWithRuntimeContext } from "@/runtime-context";
import { createIsolatedCliTestEnv } from "@/test-utils/test-process-env";
import { send_agent_message } from "@/tools/impl/send-agent-message";

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("tools retain exclusive ownership, accept queued sends, and preserve real results", async () => {
  const storageDir = temporaryDirectory();
  const owner = new LocalBackend({
    storageDir,
    executor: new DeterministicToolCallExecutor(),
    memfsEnabled: false,
  });
  const agent = await owner.createAgent({ name: "Owner" });
  await collect(
    await owner.createConversationMessageStream(
      "default",
      input(agent.id, "run a tool"),
    ),
  );
  const observer = new LocalBackend({
    storageDir,
    executionMode: "deterministic",
    memfsEnabled: false,
  });
  const body = input(agent.id, "unrelated input");
  await expect(
    observer.createConversationMessageStream("default", body),
  ).rejects.toThrow("already has an active run");
  await expect(
    observer.localConversations.assertAvailable("default", agent.id),
  ).rejects.toThrow("already has an active run");
  await expect(
    observer.compactConversationMessages("default", { agent_id: agent.id }),
  ).rejects.toThrow("already has an active run");
  await expect(
    observer.recompileConversation("default", { agent_id: agent.id }),
  ).rejects.toThrow("already has an active run");
  await expect(
    owner.localConversations.assertAvailable("default", agent.id),
  ).rejects.toThrow("already has an active run");
  await expect(observer.cancelConversation(agent.id)).rejects.toThrow(
    "already has an active run",
  );
  await expect(
    owner.createConversationMessageStream("default", body),
  ).rejects.toThrow("already has an active run");

  let launches = 0;
  const result = await runWithRuntimeContext(
    { agentId: "agent-sender", conversationId: "default" },
    () =>
      send_agent_message(
        {
          agent_id: agent.id,
          conversation_id: "default",
          message: "Child update",
        },
        {
          backend: observer,
          spawnLocal: () => {
            launches += 1;
          },
        },
      ),
  );
  expect(result.status).toBe("success");
  expect(JSON.parse(result.content).status).toBe("queued");
  expect(launches).toBe(0);
  const before = new LocalStore(agent.id, { storageDir }).listLocalMessages(
    "default",
    agent.id,
  );
  expect(
    before.filter((message) => message.role === "toolResult"),
  ).toHaveLength(0);
  const tool = before
    .flatMap((message) => (message.role === "assistant" ? message.content : []))
    .find((part) => part.type === "toolCall");
  assert(tool?.type === "toolCall");
  await collect(
    await owner.createConversationMessageStream("default", {
      agent_id: agent.id,
      messages: [
        {
          type: "approval",
          approvals: [
            {
              type: "tool",
              tool_call_id: tool.id,
              status: "success",
              tool_return: "Actual successful result",
            },
          ],
        },
      ],
    }),
  );
  const messages = new LocalStore(agent.id, { storageDir }).listLocalMessages(
    "default",
    agent.id,
  );
  const toolResult = messages.find((message) => message.role === "toolResult");
  expect(toolResult?.role === "toolResult" && toolResult.isError).toBe(false);
  expect(toolResult?.content).toEqual([
    { type: "text", text: "Actual successful result" },
  ]);
  expect(
    messages
      .filter((message) => message.role === "user")
      .map((message) => message.content),
  ).toContainEqual([
    { type: "text", text: expect.stringContaining("Child update") },
  ]);
  const resultIndex = messages.findIndex(
    (message) => message.role === "toolResult",
  );
  expect(
    messages.findIndex(
      (message) =>
        message.role === "user" &&
        JSON.stringify(message.content).includes("Child update"),
    ),
  ).toBeGreaterThan(resultIndex);
  await observer.localConversations.assertAvailable("default", agent.id);
});

test("a provider stop chunk does not release ownership before its stream tail finishes", async () => {
  const storageDir = temporaryDirectory();
  const tail = Promise.withResolvers<void>();
  const reachedTail = Promise.withResolvers<void>();
  const owner = new HeadlessBackend(
    "agent-owner",
    {
      async execute() {
        return new Stream<LettaStreamingResponse>(async function* () {
          yield {
            message_type: "assistant_message",
            id: "message-stream-tail",
            date: new Date().toISOString(),
            content: [{ type: "text", text: "reply" }],
          };
          yield { message_type: "stop_reason", stop_reason: "end_turn" };
          reachedTail.resolve();
          await tail.promise;
        }, new AbortController());
      },
    },
    { storageDir },
  );
  const other = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const draining = collect(
    await owner.createConversationMessageStream(
      "default",
      input("agent-owner", "first"),
    ),
  );
  await reachedTail.promise;
  try {
    await expect(
      other.createConversationMessageStream(
        "default",
        input("agent-owner", "overlap"),
      ),
    ).rejects.toThrow("already has an active run");
  } finally {
    tail.resolve();
    await draining;
  }
  await collect(
    await other.createConversationMessageStream(
      "default",
      input("agent-owner", "after tail"),
    ),
  );
});

test("queued provider startup is cancelled and cannot write after ownership changes", async () => {
  const storageDir = temporaryDirectory();
  const started = Promise.withResolvers<void>();
  const provider = Promise.withResolvers<Stream<LettaStreamingResponse>>();
  let attempts = 0;
  const owner = new HeadlessBackend(
    "agent-owner",
    {
      async execute() {
        attempts += 1;
        if (attempts === 1) {
          return createAssistantMessageStream();
        }
        started.resolve();
        return provider.promise;
      },
    },
    { storageDir },
  );
  const other = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await owner.createConversationMessageStream(
    "default",
    input("agent-owner", "first"),
  );
  await other.localConversations.enqueue(
    "default",
    "agent-owner",
    "queued update",
    "startup-update",
  );
  const draining = collect(stream);
  void draining.catch(() => {});
  await started.promise;
  stream.controller.abort();
  await expect(draining).rejects.toThrow();
  await collect(
    await other.createConversationMessageStream(
      "default",
      input("agent-owner", "replacement"),
    ),
  );
  const late = createAssistantMessageStream({
    content: [{ type: "text", text: "must not be persisted" }],
  });
  provider.resolve(late);
  await new Promise<void>((resolveTick) => {
    setImmediate(resolveTick);
  });
  expect(late.controller.signal.aborted).toBe(true);
  const messages = new LocalStore("agent-owner", {
    storageDir,
  }).listLocalMessages("default");
  expect(JSON.stringify(messages)).not.toContain("must not be persisted");
  expect(
    messages.filter(
      (message) => message.role === "user" && message.otid === "startup-update",
    ),
  ).toHaveLength(1);
});

test("waiting detached sends can be aborted without stealing the active owner's lease", async () => {
  const storageDir = temporaryDirectory();
  const owner = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const waiter = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await owner.createConversationMessageStream(
    "default",
    input("agent-owner", "first"),
  );
  const controller = new AbortController();
  const waiting = waiter.localConversations.wait(
    "default",
    "agent-owner",
    controller.signal,
  );
  void waiting.catch(() => {});
  controller.abort();
  await expect(waiting).rejects.toThrow();
  await expect(
    waiter.localConversations.assertAvailable("default", "agent-owner"),
  ).rejects.toThrow("already has an active run");
  await collect(stream);
  await collect(
    await waiter.createConversationMessageStream(
      "default",
      input("agent-owner", "after wait cancellation"),
    ),
  );
});

test("queued updates are delivered in order before a terminal owner releases", async () => {
  const storageDir = temporaryDirectory();
  const owner = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const sender = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await owner.createConversationMessageStream(
    "default",
    input("agent-owner", "first"),
  );
  for (const [index, content] of ["one", "two", "three"].entries()) {
    expect(
      await sender.localConversations.enqueue(
        "default",
        "agent-owner",
        content,
        `update-${index}`,
      ),
    ).toBe(true);
  }
  await collect(stream);
  const messages = new LocalStore("agent-owner", {
    storageDir,
  }).listLocalMessages("default");
  expect(
    messages
      .filter((message) => message.role === "user")
      .map((message) => message.content),
  ).toEqual(
    ["first", "one", "two", "three"].map((text) => [{ type: "text", text }]),
  );
  expect(
    messages.filter((message) => message.role === "assistant"),
  ).toHaveLength(2);
  expect(
    await sender.localConversations.enqueue(
      "default",
      "agent-owner",
      "idle send",
      "idle-send",
    ),
  ).toBe(false);
});

test("an error retains queued input durably and retry does not duplicate already persisted input", async () => {
  const storageDir = temporaryDirectory();
  let attempts = 0;
  const owner = new HeadlessBackend(
    "agent-owner",
    {
      async execute() {
        attempts += 1;
        if (attempts === 2) {
          throw new Error("provider unavailable after input persistence");
        }
        return createAssistantMessageStream();
      },
    },
    { storageDir },
  );
  const sender = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await owner.createConversationMessageStream(
    "default",
    input("agent-owner", "first"),
  );
  await sender.localConversations.enqueue(
    "default",
    "agent-owner",
    "queued update",
    "durable-update",
  );
  await expect(collect(stream)).rejects.toThrow("provider unavailable");
  const recovered = new HeadlessBackend("agent-owner", undefined, {
    storageDir,
  });
  await collect(
    await recovered.createConversationMessageStream(
      "default",
      input("agent-owner", "resume"),
    ),
  );
  const messages = new LocalStore("agent-owner", {
    storageDir,
  }).listLocalMessages("default");
  expect(
    messages.filter(
      (message) => message.role === "user" && message.otid === "durable-update",
    ),
  ).toHaveLength(1);
});

test("breaking stream iteration releases ownership and prevents late writes", async () => {
  const storageDir = temporaryDirectory();
  const first = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const second = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await first.createConversationMessageStream(
    "default",
    input("agent-owner", "first"),
  );
  for await (const _chunk of stream) {
    break;
  }
  await collect(
    await second.createConversationMessageStream(
      "default",
      input("agent-owner", "second"),
    ),
  );
  expect((await second.latestConversationRun("default"))?.status).toBe(
    "completed",
  );
});

test("aborting an unconsumed stream releases ownership and fences its late output", async () => {
  const storageDir = temporaryDirectory();
  const first = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const second = new HeadlessBackend("agent-owner", undefined, { storageDir });
  const stream = await first.createConversationMessageStream(
    "default",
    input("agent-owner", "aborted"),
  );
  stream.controller.abort();
  await first.localConversations.assertAvailable("default", "agent-owner");
  await collect(
    await second.createConversationMessageStream(
      "default",
      input("agent-owner", "second"),
    ),
  );
  await expect(collect(stream)).rejects.toThrow();
  expect(
    new LocalStore("agent-owner", { storageDir })
      .listLocalMessages("default")
      .filter((message) => message.role === "assistant"),
  ).toHaveLength(1);
});

test("different agents' default conversations execute independently", async () => {
  const storageDir = temporaryDirectory();
  const first = new HeadlessBackend("agent-first", undefined, { storageDir });
  const second = new HeadlessBackend("agent-second", undefined, { storageDir });
  const firstStream = await first.createConversationMessageStream(
    "default",
    input("agent-first", "first"),
  );
  await collect(
    await second.createConversationMessageStream(
      "default",
      input("agent-second", "second"),
    ),
  );
  await collect(firstStream);
});

test("cancelled approval ownership is released by the owner", async () => {
  const storageDir = temporaryDirectory();
  const owner = new HeadlessBackend(
    "agent-owner",
    new DeterministicToolCallExecutor(),
    { storageDir },
  );
  await collect(
    await owner.createConversationMessageStream(
      "default",
      input("agent-owner", "tool"),
    ),
  );
  await owner.cancelConversation("agent-owner");
  const next = new HeadlessBackend("agent-owner", undefined, { storageDir });
  await collect(
    await next.createConversationMessageStream(
      "default",
      input("agent-owner", "after cancellation"),
    ),
  );
});

test.each(["run", "conversation", "current conversation"])(
  "scoped %s cancellation releases client-side tool ownership",
  async (route) => {
    const storageDir = temporaryDirectory();
    const owner = new HeadlessBackend(
      "agent-owner",
      new DeterministicToolCallExecutor(),
      { storageDir },
    );
    await collect(
      await owner.createConversationMessageStream(
        "default",
        input("agent-owner", "tool"),
      ),
    );
    const run = await owner.latestConversationRun("default");
    assert(run);
    const result =
      route === "run"
        ? await owner.cancelRun("agent-owner", run.id)
        : await owner.cancelConversationRun(
            "default",
            route === "conversation" ? run.id : undefined,
          );
    expect(result).toMatchObject({ [run.id]: "cancelled" });
    expect(await owner.retrieveRun(run.id)).toMatchObject({
      status: "cancelled",
    });
    const next = new HeadlessBackend("agent-owner", undefined, { storageDir });
    await collect(
      await next.createConversationMessageStream(
        "default",
        input("agent-owner", "after scoped cancellation"),
      ),
    );
  },
);

test("a real live process cannot be reaped by age; crash recovery keeps queued notifications", async () => {
  const storageDir = temporaryDirectory();
  const worker = await startOwnerProcess(storageDir);
  try {
    const lockPath = join(
      storageDir,
      "ownership",
      Buffer.from("default:agent-owner").toString("base64url"),
      "owner.lock",
    );
    const lease: unknown = JSON.parse(readFileSync(lockPath, "utf8"));
    assert(typeof lease === "object" && lease !== null);
    writeFileSync(lockPath, JSON.stringify({ ...lease, acquiredAt: 0 }));
    const contender = new HeadlessBackend("agent-owner", undefined, {
      storageDir,
    });
    await expect(
      contender.createConversationMessageStream(
        "default",
        input("agent-owner", "must not enter"),
      ),
    ).rejects.toThrow("already has an active run");
    await expect(contender.cancelConversation("agent-owner")).rejects.toThrow(
      "already has an active run",
    );
    expect(
      await contender.localConversations.enqueue(
        "default",
        "agent-owner",
        "survives a crash",
        "crash-update",
      ),
    ).toBe(true);
    worker.kill("SIGKILL");
    await worker.exited;
    await collect(
      await contender.createConversationMessageStream(
        "default",
        input("agent-owner", "resume"),
      ),
    );
    const messages = new LocalStore("agent-owner", {
      storageDir,
    }).listLocalMessages("default");
    expect(
      messages.filter((message) => message.role === "toolResult"),
    ).toHaveLength(1);
    expect(
      messages.some(
        (message) => message.role === "user" && message.otid === "crash-update",
      ),
    ).toBe(true);
  } finally {
    worker.kill();
    await worker.exited;
  }
});

test("CLI source and built Node sends wait before approval recovery", async () => {
  const storageDir = temporaryDirectory();
  const owner = new HeadlessBackend(
    "agent-owner",
    new DeterministicToolCallExecutor(),
    { storageDir },
  );
  await collect(
    await owner.createConversationMessageStream(
      "default",
      input("agent-owner", "running tool"),
    ),
  );
  const root = temporaryDirectory();
  const bundle = process.env.LETTA_TEST_CLI_BUNDLE;
  // Capture to files: Bun-to-Node pipes can drop even console.log output on some hosts.
  const stdoutPath = join(root, "stdout.log");
  const stderrPath = join(root, "stderr.log");
  const stdoutFile = openSync(stdoutPath, "w");
  const stderrFile = openSync(stderrPath, "w");
  const child = spawn(
    bundle ? "node" : process.execPath,
    [
      ...(bundle
        ? [bundle]
        : [
            "--loader=.md:text",
            "--loader=.mdx:text",
            "--loader=.txt:text",
            resolve("src/index.ts"),
          ]),
      "--backend",
      "local",
      "-p",
      "--agent",
      "agent-owner",
      "--conv",
      "default",
      "--tools=",
      "--no-skills",
      "--no-system-info-reminder",
      "independent send",
    ],
    {
      cwd: root,
      env: createIsolatedCliTestEnv({
        LETTA_CONFIG_DIR: join(root, "config"),
        LETTA_LOCAL_BACKEND_DIR: storageDir,
        LETTA_LOCAL_BACKEND_EXECUTOR: "deterministic",
        LETTA_LOCAL_SEND: "1",
        LETTA_DEBUG_TIMINGS: "1",
        LETTA_SKIP_KEYCHAIN_CHECK: "1",
        LETTA_DISABLE_MODS: "1",
      }),
      stdio: ["ignore", stdoutFile, stderrFile],
    },
  );
  const exited = new Promise<number | null>((resolveExit, reject) => {
    child.once("error", reject);
    child.once("close", resolveExit);
  });
  try {
    const deadline = Date.now() + 10000;
    while (
      !readFileSync(stderrPath, "utf8").includes("HEADLESS_CONVERSATION_READY")
    ) {
      if (child.exitCode !== null || Date.now() >= deadline) {
        throw new Error(
          `CLI did not reach conversation startup: ${readFileSync(stderrPath, "utf8")}`,
        );
      }
      await new Promise<void>((resolveTick) => {
        setTimeout(resolveTick, 10);
      });
    }
    const before = new LocalStore("agent-owner", {
      storageDir,
    }).listLocalMessages("default");
    expect(
      before.filter((message) => message.role === "toolResult"),
    ).toHaveLength(0);
    const tool = before
      .flatMap((message) =>
        message.role === "assistant" ? message.content : [],
      )
      .find((part) => part.type === "toolCall");
    assert(tool?.type === "toolCall");
    await collect(
      await owner.createConversationMessageStream("default", {
        agent_id: "agent-owner",
        messages: [
          {
            type: "approval",
            approvals: [
              {
                type: "tool",
                tool_call_id: tool.id,
                status: "success",
                tool_return: "Tool completed while another CLI was waiting",
              },
            ],
          },
        ],
      }),
    );
    const exitCode = await exited;
    const stdout = readFileSync(stdoutPath, "utf8");
    const stderr = readFileSync(stderrPath, "utf8");
    expect(exitCode, stderr).toBe(0);
    expect(stdout, stderr).toContain("pong");
    const messages = new LocalStore("agent-owner", {
      storageDir,
    }).listLocalMessages("default");
    const completedTool = messages.find(
      (message) => message.role === "toolResult",
    );
    expect(completedTool?.role === "toolResult" && completedTool.isError).toBe(
      false,
    );
    expect(completedTool?.content).toEqual([
      { type: "text", text: "Tool completed while another CLI was waiting" },
    ]);
    expect(
      messages.filter(
        (message) =>
          message.role === "user" &&
          JSON.stringify(message.content).includes("independent send"),
      ),
    ).toHaveLength(1);
  } finally {
    if (child.exitCode === null) {
      child.kill();
    }
    await exited;
    closeSync(stdoutFile);
    closeSync(stderrFile);
  }
}, 30000);

function temporaryDirectory(): string {
  const directory = mkdtempSync(join(tmpdir(), "local-ownership-"));
  directories.push(directory);
  return directory;
}

function input(agentId: string, content: string) {
  return { agent_id: agentId, messages: [{ role: "user" as const, content }] };
}

async function collect(stream: AsyncIterable<unknown>): Promise<void> {
  for await (const _chunk of stream) {
  }
}

async function startOwnerProcess(storageDir: string) {
  const source = `
    import { HeadlessBackend } from ${JSON.stringify(resolve("src/backend/dev/fake-headless-backend.ts"))};
    import { DeterministicToolCallExecutor } from ${JSON.stringify(resolve("src/backend/dev/headless-turn-executor.ts"))};
    const backend = new HeadlessBackend("agent-owner", new DeterministicToolCallExecutor(), { storageDir: ${JSON.stringify(storageDir)} });
    for await (const chunk of await backend.createConversationMessageStream("default", { agent_id: "agent-owner", messages: [{role: "user", content: "running tool"}] })) {}
    console.log("ready");
    await Bun.stdin.text();
  `;
  const child = Bun.spawn([process.execPath, "--eval", source], {
    cwd: process.cwd(),
    stdin: "pipe",
    stdout: "pipe",
    stderr: "pipe",
  });
  const reader = child.stdout.getReader();
  const ready = await reader.read();
  reader.releaseLock();
  expect(new TextDecoder().decode(ready.value)).toContain("ready");
  return child;
}
