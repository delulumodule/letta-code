import { afterEach, expect, test } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HeadlessBackend } from "@/backend/dev/fake-headless-backend";
import { createAssistantMessageStream } from "@/backend/dev/headless-turn-executor";
import { LocalBackend } from "@/backend/local/local-backend";
import { LocalStore } from "@/backend/local/local-store";

const directories: string[] = [];
afterEach(() => {
  for (const directory of directories.splice(0)) {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("production LocalBackend must reject overlapping owners of one conversation", async () => {
  const storageDir = mkdtempSync(join(tmpdir(), "letta-production-repro-"));
  directories.push(storageDir);
  const first = new LocalBackend({
    storageDir,
    executionMode: "deterministic",
    memfsEnabled: false,
  });
  const agent = await first.createAgent({ name: "Repro" });
  const conversation = await first.createConversation({ agent_id: agent.id });
  const body = {
    agent_id: agent.id,
    messages: [{ role: "user" as const, content: "hello" }],
  };
  const firstStream = await first.createConversationMessageStream(
    conversation.id,
    body,
  );
  const second = new LocalBackend({
    storageDir,
    executionMode: "deterministic",
    memfsEnabled: false,
  });
  let rejected = false;
  try {
    const secondStream = await second.createConversationMessageStream(
      conversation.id,
      body,
    );
    for await (const _chunk of secondStream) {
    }
  } catch {
    rejected = true;
  } finally {
    for await (const _chunk of firstStream) {
    }
  }
  expect(rejected).toBe(true);
});

test("control: a single LocalBackend rejects the overlapping turn", async () => {
  const storageDir = mkdtempSync(join(tmpdir(), "letta-single-owner-control-"));
  directories.push(storageDir);
  const backend = new LocalBackend({
    storageDir,
    executionMode: "deterministic",
    memfsEnabled: false,
  });
  const agent = await backend.createAgent({ name: "Repro" });
  const conversation = await backend.createConversation({ agent_id: agent.id });
  const body = {
    agent_id: agent.id,
    messages: [{ role: "user" as const, content: "hello" }],
  };
  const stream = await backend.createConversationMessageStream(
    conversation.id,
    body,
  );
  try {
    await expect(
      backend.createConversationMessageStream(conversation.id, body),
    ).rejects.toThrow("already has an active run");
  } finally {
    for await (const _chunk of stream) {
    }
  }
});

test("successive owners preserve independent replies when an existing backend resumes", async () => {
  const storageDir = mkdtempSync(join(tmpdir(), "local-owner-handoff-"));
  directories.push(storageDir);
  const first = new HeadlessBackend(
    "agent-repro",
    {
      async execute() {
        return createAssistantMessageStream({
          content: [{ type: "text", text: "first response" }],
        });
      },
    },
    { storageDir },
  );
  const second = new HeadlessBackend(
    "agent-repro",
    {
      async execute() {
        return createAssistantMessageStream({
          content: [{ type: "text", text: "second response" }],
        });
      },
    },
    { storageDir },
  );
  const body = {
    agent_id: "agent-repro",
    messages: [{ role: "user" as const, content: "hello" }],
  };
  const firstStream = await first.createConversationMessageStream(
    "default",
    body,
  );
  await expect(
    second.createConversationMessageStream("default", body),
  ).rejects.toThrow("already has an active run");
  for await (const _chunk of firstStream) {
  }
  for await (const _chunk of await second.createConversationMessageStream(
    "default",
    body,
  )) {
  }
  for await (const _chunk of await first.createConversationMessageStream(
    "default",
    body,
  )) {
  }
  const messages = new LocalStore("agent-repro", {
    storageDir,
  }).listLocalMessages("default", "agent-repro");
  const assistants = messages.filter((message) => message.role === "assistant");
  expect(assistants.map((message) => message.content)).toEqual([
    [{ type: "text", text: "first response" }],
    [{ type: "text", text: "second response" }],
    [{ type: "text", text: "first response" }],
  ]);
  expect(new Set(messages.map((message) => message.id)).size).toBe(
    messages.length,
  );
});
