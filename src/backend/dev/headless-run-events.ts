import type { LettaStreamingResponse } from "@letta-ai/letta-client/resources/agents/messages";
import type { LocalProviderErrorInfo } from "./local-provider-errors";

export function runStopReason(
  chunk: LettaStreamingResponse,
): string | undefined {
  if (chunk.message_type !== "stop_reason") {
    return undefined;
  }
  const stopReason = (chunk as { stop_reason?: unknown }).stop_reason;
  return typeof stopReason === "string" ? stopReason : undefined;
}

export type RunErrorMetadata = Pick<
  LocalProviderErrorInfo,
  "message" | "detail" | "error_type" | "retryable"
> & { run_id?: string };

export function runErrorInfo(
  chunk: LettaStreamingResponse,
): RunErrorMetadata | undefined {
  if (chunk.message_type !== "error_message") {
    return undefined;
  }
  const record = chunk as {
    message?: unknown;
    detail?: unknown;
    error_type?: unknown;
    retryable?: unknown;
  };
  const message = typeof record.message === "string" ? record.message : "";
  const detail =
    typeof record.detail === "string"
      ? record.detail
      : message || "Unknown local backend error";
  const errorType =
    record.error_type === "llm_error" ||
    record.error_type === "local_backend_error"
      ? record.error_type
      : "local_backend_error";
  return {
    message: message || detail,
    detail,
    error_type: errorType,
    retryable: record.retryable === true,
  };
}

export function localStreamProtocolError(message: string): RunErrorMetadata {
  return {
    message,
    detail: message,
    error_type: "local_backend_error",
    retryable: false,
  };
}

export function localErrorChunk(
  info: RunErrorMetadata,
): LettaStreamingResponse {
  return {
    message_type: "error_message",
    message: info.message,
    detail: info.detail,
    error_type: info.error_type,
    retryable: info.retryable,
  } as unknown as LettaStreamingResponse;
}

export function localStopReasonChunk(
  stopReason: string,
): LettaStreamingResponse {
  return {
    message_type: "stop_reason",
    stop_reason: stopReason,
  } as LettaStreamingResponse;
}
