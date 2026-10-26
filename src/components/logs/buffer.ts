import type { LogLine } from "../../api/types";

/** Больше стольких строк интерфейс не держит (спец. §6.6): в follow старые уходят, как в кольцевом буфере. */
export const MAX_LINES = 100_000;

/** Строки буфера после добавления `lines`: последние `max`. */
export function appendLines(buffer: LogLine[], lines: LogLine[], max = MAX_LINES): LogLine[] {
  const next = buffer.concat(lines);
  return next.length > max ? next.slice(next.length - max) : next;
}
