import { expect, mock, test } from "bun:test";
import assert from "node:assert/strict";
import { setTelegramGroupTrigger } from "@/channels/telegram/group-trigger";
import type { InboundChannelMessage } from "@/channels/types";
import { buildChannelNotificationXml } from "@/channels/xml";
import {
  createTelegramAdapter,
  detectTelegramBotMention,
  FakeBot,
  installTelegramAdapterTestHooks,
  telegramAccountDefaults,
  withTimeout,
} from "./telegram/adapter-test-harness";

installTelegramAdapterTestHooks();

test("telegram adapter forwards plain text messages through onMessage", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: 123 },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "Hello from Telegram",
      date: 1_736_380_800,
      message_id: 77,
    },
  });

  expect(onMessage).toHaveBeenCalledWith({
    channel: "telegram",
    accountId: "telegram-test-account",
    chatId: "123",
    senderId: "456",
    senderName: "alice",
    text: "Hello from Telegram",
    isMention: false,
    timestamp: 1_736_380_800_000,
    messageId: "77",
    chatType: "direct",
    attachments: undefined,
    raw: expect.objectContaining({ message_id: 77 }),
  });
});

test("telegram delivers a structured rich message without legacy text", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });
  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;
  await adapter.start();

  await FakeBot.instances[0]?.emit("message", {
    message: {
      chat: { id: 123, type: "private" },
      from: { id: 456, username: "alice" },
      date: 1_736_380_800,
      message_id: 79,
      rich_message: {
        blocks: [
          { type: "heading", size: 2, text: "Comparison" },
          {
            type: "table",
            cells: [
              [{ text: "Model" }, { text: "Price" }],
              [
                {
                  text: {
                    type: "url",
                    text: "Charger",
                    url: "https://example.com/item",
                  },
                },
                { text: ["€", { type: "bold", text: "29" }] },
              ],
            ],
          },
          {
            type: "details",
            summary: "Notes",
            blocks: [
              {
                type: "list",
                items: [
                  {
                    label: "1.",
                    blocks: [{ type: "paragraph", text: "USB-C" }],
                  },
                  {
                    label: "2.",
                    blocks: [
                      { type: "paragraph", text: "Compact" },
                      { type: "paragraph", text: "Fits in a pocket" },
                    ],
                  },
                ],
              },
              {
                type: "photo",
                caption: { text: "Side view", credit: "Example" },
              },
            ],
          },
        ],
      },
    },
  });

  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      messageId: "79",
      text: [
        "Comparison",
        "Model | Price\nCharger (https://example.com/item) | €29",
        "Notes\n1. USB-C\n2. Compact\n   Fits in a pocket\n\nSide view\nExample",
      ].join("\n\n"),
    }),
  );
});

test("Telegram topic triggers update live and leave neighboring topics and DMs alone", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });
  const received: InboundChannelMessage[] = [];
  adapter.onMessage = async (message) => {
    received.push(message);
  };
  await adapter.start();

  const accountId = telegramAccountDefaults.accountId;
  const group = { accountId, chatId: "-100123" };
  const topic = { ...group, threadId: "42" };
  setTelegramGroupTrigger({ accountId }, { mode: "pattern", pattern: "misa" });
  setTelegramGroupTrigger(group, { mode: "pattern", pattern: "group keyword" });
  setTelegramGroupTrigger(topic, { mode: "all" });
  expect(() =>
    setTelegramGroupTrigger(topic, { mode: "pattern", pattern: "[" }),
  ).toThrow();

  const base = {
    chat: { id: -100123, type: "supergroup" },
    from: { id: 456 },
    text: "ambient",
    date: 1_736_380_800,
  };
  const bot = FakeBot.instances[0];
  assert(bot);
  await bot.emit("message", {
    message: { ...base, message_thread_id: 42, message_id: 80 },
  });
  await bot.emit("message", {
    message: { ...base, message_thread_id: 43, message_id: 81 },
  });
  await bot.emit("message", {
    message: {
      ...base,
      message_thread_id: 43,
      message_id: 82,
      text: "GROUP keyword",
    },
  });
  await bot.emit("message", {
    message: {
      ...base,
      chat: { id: -100124, type: "supergroup" },
      message_id: 83,
      text: "MISA",
    },
  });
  await bot.emit("message", {
    message: {
      ...base,
      message_thread_id: 43,
      message_id: 84,
      reply_to_message: {
        ...base,
        message_id: 70,
        from: { id: bot.botInfo.id },
      },
    },
  });
  await bot.emit("message", {
    message: {
      ...base,
      chat: { id: 456, type: "private" },
      message_id: 85,
    },
  });
  setTelegramGroupTrigger(topic, undefined);
  await bot.emit("message", {
    message: { ...base, message_thread_id: 42, message_id: 86 },
  });

  expect(
    received.map(({ messageId, isOpenChannel }) => [messageId, isOpenChannel]),
  ).toEqual([
    ["80", true],
    ["82", true],
    ["83", true],
    ["84", true],
    ["85", undefined],
  ]);
});

test("telegram adapter preserves group topic metadata on inbound messages", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
      message_thread_id: 42,
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "Hello from topic",
      date: 1_736_380_800,
      message_id: 77,
    },
  });

  expect(onMessage).toHaveBeenCalledWith({
    channel: "telegram",
    accountId: "telegram-test-account",
    chatId: "-100123",
    senderId: "456",
    senderName: "alice",
    chatLabel: "Void Cafe",
    text: "Hello from topic",
    isMention: false,
    timestamp: 1_736_380_800_000,
    messageId: "77",
    threadId: "42",
    chatType: "channel",
    attachments: undefined,
    raw: expect.objectContaining({ message_id: 77 }),
  });
});

test("telegram adapter detects bot mentions and strips a leading mention", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "@test_bot: hello from topic",
      entities: [{ type: "mention", offset: 0, length: 9 }],
      date: 1_736_380_800,
      message_id: 77,
    },
  });

  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      chatId: "-100123",
      chatType: "channel",
      isMention: true,
      text: "hello from topic",
    }),
  );
});

test("telegram adapter forwards reply context for quoted messages", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "@test_bot please respond",
      entities: [{ type: "mention", offset: 0, length: 9 }],
      date: 1_736_380_800,
      message_id: 78,
      reply_to_message: {
        chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
        from: { id: 789, username: "blink", first_name: "Blink" },
        text: "Am I allowed as this user to mutate your configuration?",
        date: 1_736_380_790,
        message_id: 77,
      },
    },
  });

  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      text: "please respond",
      isMention: true,
      replyContext: {
        messageId: "77",
        senderId: "789",
        senderName: "blink",
        text: "Am I allowed as this user to mutate your configuration?",
      },
    }),
  );
});

test.each([
  { id: 123, type: "private" },
  { id: -100123, type: "supergroup" },
])(
  "Telegram delivers a selected rich quote immediately in a $type chat",
  async (chat) => {
    const adapter = createTelegramAdapter({
      ...telegramAccountDefaults,
      channel: "telegram",
      token: "test-token",
      enabled: true,
      dmPolicy: "pairing",
      allowedUsers: [],
      inboundDebounceMs: 100,
    });
    const notifications: string[] = [];
    adapter.onMessage = async (message) => {
      notifications.push(buildChannelNotificationXml(message));
    };
    await adapter.start();

    await FakeBot.instances[0]?.emit("message", {
      message: {
        chat,
        from: { id: 456, username: "alice" },
        text: "Is it available?",
        date: 1_736_380_800,
        message_id: 78,
        quote: {
          text: "",
          rich_texts: [
            ["Compact charger — ", { type: "bold", text: "29 €" }],
            "USB-C & 6 mm",
          ],
          position: 48,
          is_manual: true,
        },
        reply_to_message: {
          chat,
          from: { id: 12345, username: "test_bot" },
          date: 1_736_380_790,
          message_id: 77,
          rich_message: { blocks: [] },
        },
      },
    });

    expect(notifications).toHaveLength(1);
    expect(notifications[0]).toContain(
      '<reply-context message_id="77" sender_id="12345" sender_name="test_bot" quoted="true">\n' +
        "Compact charger — 29 €\nUSB-C &amp; 6 mm\n</reply-context>",
    );
    expect(notifications[0]).toContain("Is it available?");
  },
);

test("detectTelegramBotMention preserves non-leading mentions", () => {
  expect(
    detectTelegramBotMention(
      {
        chat: { id: 1, type: "supergroup" },
        from: { id: 2 },
        text: "hey @test_bot can you see this",
        entities: [{ type: "mention", offset: 4, length: 9 }],
        date: 1,
        message_id: 1,
      },
      "test_bot",
    ),
  ).toEqual({
    isMention: true,
    text: "hey @test_bot can you see this",
  });
});

test("detectTelegramBotMention accepts leading bot display name", () => {
  expect(
    detectTelegramBotMention(
      {
        chat: { id: 1, type: "supergroup" },
        from: { id: 2 },
        text: "Void, what am I quoting?",
        date: 1,
        message_id: 1,
      },
      "void_comind_bot",
      "Void",
    ),
  ).toEqual({
    isMention: true,
    text: "what am I quoting?",
  });
});

test("telegram adapter debounces group bursts by chat/topic", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
    inboundDebounceMs: 20,
  });

  const received: InboundChannelMessage[] = [];
  const onMessage = mock(async (message: InboundChannelMessage) => {
    received.push(message);
  });
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
      message_thread_id: 42,
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "first",
      date: 1_736_380_800,
      message_id: 77,
    },
  });
  await bot?.emit("message", {
    message: {
      chat: { id: -100123, type: "supergroup", title: "Void Cafe" },
      message_thread_id: 42,
      from: { id: 789, username: "bob", first_name: "Bob" },
      text: "second",
      date: 1_736_380_801,
      message_id: 78,
    },
  });

  await withTimeout(
    new Promise<void>((resolve) => {
      const check = () => {
        if (received.length === 1) resolve();
        else setTimeout(check, 5);
      };
      check();
    }),
    500,
    "Timed out waiting for Telegram debounce flush",
  );

  expect(onMessage).toHaveBeenCalledTimes(1);
  expect(received[0]).toMatchObject({
    chatId: "-100123",
    threadId: "42",
    chatType: "channel",
    senderId: "789",
    messageId: "78",
    text: "alice: first\nbob: second",
  });
});

test("telegram adapter does not debounce direct messages", async () => {
  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
    inboundDebounceMs: 100,
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: 123, type: "private" },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "dm one",
      date: 1_736_380_800,
      message_id: 77,
    },
  });
  await bot?.emit("message", {
    message: {
      chat: { id: 123, type: "private" },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "dm two",
      date: 1_736_380_801,
      message_id: 78,
    },
  });

  expect(onMessage).toHaveBeenCalledTimes(2);
});

test("telegram adapter transcribes inbound voice memos when opt-in is enabled", async () => {
  process.env.OPENAI_API_KEY = "sk-test";

  globalThis.fetch = mock(async (url: string | URL | Request) => {
    const href = typeof url === "string" ? url : url.toString();

    if (href.includes("/file/bottest-token/voice/voice1.ogg")) {
      return new Response(Buffer.from("voice-bytes"), {
        status: 200,
        headers: { "content-type": "audio/ogg" },
      });
    }

    if (href === "https://api.openai.com/v1/audio/transcriptions") {
      return new Response(JSON.stringify({ text: "Transcribed voice memo" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }

    throw new Error(`Unexpected fetch URL: ${href}`);
  }) as unknown as typeof fetch;

  FakeBot.nextGetFileImpl = async () => ({
    file_path: "voice/voice1.ogg",
  });

  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
    transcribeVoice: true,
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: 123 },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "",
      date: 1_736_380_800,
      message_id: 77,
      voice: {
        file_id: "voice1",
        file_unique_id: "voice-unique-1",
        mime_type: "audio/ogg",
        file_size: 12,
      },
    },
  });

  expect(onMessage).toHaveBeenCalledTimes(1);
  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      attachments: [
        expect.objectContaining({
          kind: "audio",
          mimeType: "audio/ogg",
          transcription: "Transcribed voice memo",
        }),
      ],
    }),
  );
});

test("telegram adapter skips voice transcription unless opt-in is enabled", async () => {
  process.env.OPENAI_API_KEY = "sk-test";

  globalThis.fetch = mock(async (url: string | URL | Request) => {
    const href = typeof url === "string" ? url : url.toString();

    if (href.includes("/file/bottest-token/voice/voice1.ogg")) {
      return new Response(Buffer.from("voice-bytes"), {
        status: 200,
        headers: { "content-type": "audio/ogg" },
      });
    }

    if (href === "https://api.openai.com/v1/audio/transcriptions") {
      throw new Error(
        "Whisper should not be called when transcription is disabled",
      );
    }

    throw new Error(`Unexpected fetch URL: ${href}`);
  }) as unknown as typeof fetch;

  FakeBot.nextGetFileImpl = async () => ({
    file_path: "voice/voice1.ogg",
  });

  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  await adapter.start();

  const bot = FakeBot.instances[0];
  await bot?.emit("message", {
    message: {
      chat: { id: 123 },
      from: { id: 456, username: "alice", first_name: "Alice" },
      text: "",
      date: 1_736_380_800,
      message_id: 77,
      voice: {
        file_id: "voice1",
        file_unique_id: "voice-unique-1",
        mime_type: "audio/ogg",
        file_size: 12,
      },
    },
  });

  expect(onMessage).toHaveBeenCalledTimes(1);
  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      attachments: [
        expect.not.objectContaining({
          transcription: expect.any(String),
        }),
      ],
    }),
  );
});

test("telegram adapter exposes inbound voice transcription errors", async () => {
  process.env.OPENAI_API_KEY = "sk-test";
  const warn = console.warn;
  console.warn = mock(() => {}) as unknown as typeof console.warn;

  globalThis.fetch = mock(async (url: string | URL | Request) => {
    const href = typeof url === "string" ? url : url.toString();

    if (href.includes("/file/bottest-token/voice/voice1.ogg")) {
      return new Response(Buffer.from("voice-bytes"), {
        status: 200,
        headers: { "content-type": "audio/ogg" },
      });
    }

    if (href === "https://api.openai.com/v1/audio/transcriptions") {
      return new Response("Rate limited", { status: 429 });
    }

    throw new Error(`Unexpected fetch URL: ${href}`);
  }) as unknown as typeof fetch;

  FakeBot.nextGetFileImpl = async () => ({
    file_path: "voice/voice1.ogg",
  });

  const adapter = createTelegramAdapter({
    ...telegramAccountDefaults,
    channel: "telegram",
    enabled: true,
    token: "test-token",
    dmPolicy: "pairing",
    allowedUsers: [],
    transcribeVoice: true,
  });

  const onMessage = mock(async () => {});
  adapter.onMessage = onMessage;

  try {
    await adapter.start();

    const bot = FakeBot.instances[0];
    await bot?.emit("message", {
      message: {
        chat: { id: 123 },
        from: { id: 456, username: "alice", first_name: "Alice" },
        text: "",
        date: 1_736_380_800,
        message_id: 77,
        voice: {
          file_id: "voice1",
          file_unique_id: "voice-unique-1",
          mime_type: "audio/ogg",
          file_size: 12,
        },
      },
    });
  } finally {
    console.warn = warn;
  }

  expect(onMessage).toHaveBeenCalledTimes(1);
  expect(onMessage).toHaveBeenCalledWith(
    expect.objectContaining({
      attachments: [
        expect.objectContaining({
          transcriptionError: expect.stringContaining("429"),
        }),
      ],
    }),
  );
});
