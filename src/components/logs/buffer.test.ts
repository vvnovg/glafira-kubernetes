import { describe, expect, it } from "vitest";
import type { LogLine } from "../../api/types";
import { appendLines } from "./buffer";

const line = (text: string): LogLine => ({ ts: null, text, truncated: false });
const texts = (b: LogLine[]) => b.map((l) => l.text);

describe("буфер логов", () => {
  it("держит последние строки", () => {
    const b = appendLines([line("1"), line("2")], [line("3"), line("4")], 3);
    expect(texts(b)).toEqual(["2", "3", "4"]);
    expect(appendLines([], [line("a")], 3)).toHaveLength(1);
  });

  it("пакет больше буфера оставляет последние max строк пакета", () => {
    const old = [line("1"), line("2")];
    const batch = ["a", "b", "c", "d", "e"].map(line);
    expect(texts(appendLines(old, batch, 3))).toEqual(["c", "d", "e"]);
    // Ровно max — прежние строки уходят все.
    expect(texts(appendLines(old, batch.slice(0, 3), 3))).toEqual(["a", "b", "c"]);
    // Входные массивы не меняются.
    expect(texts(old)).toEqual(["1", "2"]);
    expect(batch).toHaveLength(5);
  });

  it("пустой пакет возвращает тот же буфер, непустой — новый, старый не меняется", () => {
    const buffer = [line("1"), line("2")];
    expect(appendLines(buffer, [], 3)).toBe(buffer);
    const next = appendLines(buffer, [line("3")], 3);
    expect(next).not.toBe(buffer);
    expect(texts(buffer)).toEqual(["1", "2"]);
    expect(texts(appendLines(next, [line("4"), line("5")], 3))).toEqual(["3", "4", "5"]);
  });
});
