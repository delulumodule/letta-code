import { afterEach, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Stream } from "@letta-ai/letta-client/core/streaming";
import type { LettaStreamingResponse } from "@letta-ai/letta-client/resources/agents/messages";
import type { ConversationMessageCreateBody } from "@/backend";
import type { HeadlessTurnExecutor } from "@/backend/dev/headless-turn-executor";
import { LocalBackend } from "./local-backend";
import { LocalStore } from "./local-store";

let storageDir: string | undefined;

afterEach(async () => {
  if (storageDir) {
    await rm(storageDir, { recursive: true, force: true });
    storageDir = undefined;
  }
});

test("keeps committed memory in the system prompt across local turns", async () => {
  storageDir = await mkdtemp(join(tmpdir(), "local-backend-cache-"));
  const systemPrompts: Array<string | undefined> = [];
  const executor: HeadlessTurnExecutor = {
    async execute(input) {
      systemPrompts.push(input.systemPrompt);
      return new Stream<LettaStreamingResponse>(async function* () {
        yield {
          message_type: "assistant_message",
          id: "simulated-assistant-message",
          date: new Date().toISOString(),
          content: [{ type: "text", text: "ok" }],
        };
        yield { message_type: "stop_reason", stop_reason: "end_turn" };
      }, new AbortController());
    },
  };
  const backend = new LocalBackend({
    storageDir,
    executor,
  });
  const agent = await backend.createAgent({
    name: "Local",
    system: "base {CORE_MEMORY}",
  } as never);
  const conversation = await backend.createConversation({
    agent_id: agent.id,
  } as never);
  const memoryDir = join(storageDir, "memfs", agent.id, "memory");
  await writeFile(
    join(memoryDir, "persona.md"),
    '---\nname: "Persona"\ndescription: "Who the agent is"\n---\nChanged but not explicitly recompiled.\n',
    "utf8",
  );
  execFileSync("git", ["add", "persona.md"], { cwd: memoryDir });
  execFileSync("git", ["commit", "-m", "test memory change"], {
    cwd: memoryDir,
  });
  await drain(
    await backend.createConversationMessageStream(conversation.id, {
      agent_id: agent.id,
      messages: [{ role: "user", content: "first" }],
    } as ConversationMessageCreateBody),
  );

  await drain(
    await backend.createConversationMessageStream(conversation.id, {
      agent_id: agent.id,
      messages: [{ role: "user", content: "second" }],
    } as ConversationMessageCreateBody),
  );

  expect(systemPrompts).toHaveLength(2);
  expect(systemPrompts[1]).toContain("Changed but not explicitly recompiled.");
  expect(systemPrompts[0]).toBe(systemPrompts[1]);

  const store = new LocalStore(agent.id, { storageDir });
  const current = store.getCompiledSystemPrompt(conversation.id, agent.id);
  if (!current) {
    throw new Error("Expected a compiled system prompt");
  }
  store.setCompiledSystemPrompt(conversation.id, agent.id, {
    ...current,
    version: undefined,
    content: "base <memory>\n# Memory\n</memory>",
  });

  const restarted = new LocalBackend({ storageDir, executor });
  await drain(
    await restarted.createConversationMessageStream(conversation.id, {
      agent_id: agent.id,
      messages: [{ role: "user", content: "after upgrade" }],
    } as ConversationMessageCreateBody),
  );
  expect(systemPrompts[2]).toContain("Changed but not explicitly recompiled.");
});

async function drain(stream: AsyncIterable<unknown>): Promise<void> {
  for await (const _chunk of stream) {
    // Wait for the turn to finish before sending another message.
  }
}
