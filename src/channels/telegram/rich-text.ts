import { isRecord } from "@/utils/type-guards";

export function flattenTelegramRichText(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value)) {
    return value.map(flattenTelegramRichText).join("");
  }
  if (!isRecord(value)) {
    return "";
  }

  if (
    value.type === "custom_emoji" &&
    typeof value.alternative_text === "string"
  ) {
    return value.alternative_text;
  }
  if (
    value.type === "mathematical_expression" &&
    typeof value.expression === "string"
  ) {
    return value.expression;
  }

  const text = flattenTelegramRichText(value.text);
  if (
    value.type === "url" &&
    typeof value.url === "string" &&
    value.url !== text
  ) {
    return text ? `${text} (${value.url})` : value.url;
  }
  return text;
}

export function flattenTelegramRichBlocks(
  blocks: unknown,
  separator = "\n\n",
): string {
  return Array.isArray(blocks)
    ? blocks.map(flattenTelegramRichBlock).filter(Boolean).join(separator)
    : "";
}

function flattenTelegramRichBlock(block: unknown): string {
  if (!isRecord(block)) {
    return "";
  }

  const content =
    block.type === "table" && Array.isArray(block.cells)
      ? block.cells.map(flattenTelegramTableRow).filter(Boolean).join("\n")
      : block.type === "list" && Array.isArray(block.items)
        ? block.items.map(flattenTelegramListItem).filter(Boolean).join("\n")
        : flattenTelegramRichBlocks(block.blocks);

  return [
    flattenTelegramRichText(block.summary),
    flattenTelegramRichText(block),
    content,
    flattenTelegramRichText(block.caption),
    flattenTelegramRichText(
      isRecord(block.caption) ? block.caption.credit : block.credit,
    ),
  ]
    .filter(Boolean)
    .join("\n");
}

function flattenTelegramTableRow(row: unknown): string {
  if (!Array.isArray(row)) {
    return "";
  }
  const cells = row.map(flattenTelegramRichText);
  return cells.some(Boolean) ? cells.join(" | ") : "";
}

function flattenTelegramListItem(item: unknown): string {
  if (!isRecord(item)) {
    return "";
  }
  const text = flattenTelegramRichBlocks(item.blocks, "\n");
  if (!text) {
    return "";
  }
  const label =
    typeof item.label === "string" && item.label.trim()
      ? item.label.trim()
      : "-";
  const checkbox = item.has_checkbox ? (item.is_checked ? "[x] " : "[ ] ") : "";
  const prefix = `${label} ${checkbox}`;
  return prefix + text.replaceAll("\n", `\n${" ".repeat(prefix.length)}`);
}
