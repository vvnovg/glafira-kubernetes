import type { LogLine } from "../../api/types";

/** Больше стольких строк интерфейс не держит (спец. §6.6): в follow старые уходят, как в кольцевом буфере. */
export const MAX_LINES = 100_000;

/**
 * Строки буфера после добавления `lines`: последние `max`. Пустой пакет возвращает тот же буфер
 * (React не перерисует список), непустой — одна копия буфера на пакет, даже когда начало отбрасывается.
 */
export function appendLines(buffer: LogLine[], lines: LogLine[], max = MAX_LINES): LogLine[] {
  if (lines.length === 0) return buffer;
  // Пакет сам не меньше буфера: прежние строки уходят все, из пакета остаются последние `max`.
  if (lines.length >= max) return lines.slice(lines.length - max);
  const drop = buffer.length + lines.length - max;
  if (drop <= 0) return buffer.concat(lines);
  const next = buffer.slice(drop);
  for (const line of lines) next.push(line);
  return next;
}
