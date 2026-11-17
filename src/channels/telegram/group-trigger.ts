import { randomUUID } from "node:crypto";
import {
  mkdirSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { getChannelDir } from "@/channels/config";
import { isRecord } from "@/utils/type-guards";
import {
  extractTelegramMessageText,
  type TelegramLikeMessage,
} from "./message-shapes";

export type TelegramGroupTriggerScope = {
  accountId: string;
  chatId?: string;
  threadId?: string;
};

export type TelegramGroupTrigger =
  | { mode: "all" }
  | { mode: "pattern"; pattern: string };

export function getTelegramGroupTrigger(
  scope: TelegramGroupTriggerScope,
): TelegramGroupTrigger | undefined {
  try {
    const rule: unknown = JSON.parse(
      readFileSync(groupTriggerPath(scope), "utf8"),
    );
    if (isRecord(rule)) {
      if (rule.mode === "all") {
        return { mode: "all" };
      }
      if (
        rule.mode === "pattern" &&
        typeof rule.pattern === "string" &&
        rule.pattern
      ) {
        return { mode: "pattern", pattern: rule.pattern };
      }
    }
    throw new Error("Invalid Telegram group trigger setting");
  } catch (error) {
    if (isRecord(error) && error.code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
}

export function resolveTelegramGroupTrigger({
  accountId,
  chatId,
  threadId,
}: TelegramGroupTriggerScope): TelegramGroupTrigger | undefined {
  return (
    (threadId
      ? getTelegramGroupTrigger({ accountId, chatId, threadId })
      : undefined) ??
    (chatId ? getTelegramGroupTrigger({ accountId, chatId }) : undefined) ??
    getTelegramGroupTrigger({ accountId })
  );
}

export function setTelegramGroupTrigger(
  scope: TelegramGroupTriggerScope,
  rule: TelegramGroupTrigger | undefined,
): void {
  const path = groupTriggerPath(scope);
  if (!rule) {
    rmSync(path, { force: true });
    return;
  }

  if (rule.mode === "pattern") {
    if (!rule.pattern) {
      throw new Error("Pass a non-empty regex or use --inherit");
    }
    new RegExp(rule.pattern, "iu");
  }

  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temporaryPath = `${path}.${randomUUID()}.tmp`;
  try {
    writeFileSync(temporaryPath, `${JSON.stringify(rule)}\n`, {
      flag: "wx",
      mode: 0o600,
    });
    renameSync(temporaryPath, path);
  } finally {
    rmSync(temporaryPath, { force: true });
  }
}

export function matchesTelegramGroupTrigger(
  rule: TelegramGroupTrigger,
  messages: TelegramLikeMessage[],
  botId: number | undefined,
): boolean {
  if (rule.mode === "all") {
    return true;
  }
  const isReplyToBot =
    botId !== undefined &&
    messages.some(
      (message) => String(message.reply_to_message?.from?.id) === String(botId),
    );
  return (
    isReplyToBot ||
    new RegExp(rule.pattern, "iu").test(
      messages.map(extractTelegramMessageText).join("\n"),
    )
  );
}

function groupTriggerPath(scope: TelegramGroupTriggerScope): string {
  const key = Buffer.from(
    JSON.stringify([scope.accountId, scope.chatId, scope.threadId]),
  ).toString("base64url");
  return join(getChannelDir("telegram"), "group-triggers", `${key}.json`);
}
