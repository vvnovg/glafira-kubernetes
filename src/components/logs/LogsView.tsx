import { useVirtualizer } from "@tanstack/react-virtual";
import { save } from "@tauri-apps/plugin-dialog";
import { useEffect, useId, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { AppError, LogLine, LogParams } from "../../api/types";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import { useEpoch } from "../../state/connectionEpoch";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { ReadError } from "../common/ReadError";
import { appendLines, MAX_LINES } from "./buffer";

const ROW_HEIGHT = 18;
/** Начало лога: без ограничения или последние 5 мин, 1 ч, 6 ч, 24 ч (секунды). */
const SINCE = [0, 300, 3600, 21_600, 86_400];
/** Через сколько после ввода хвост и фильтр применяются: сессия не перезапускается на каждую клавишу. */
const TYPING_DELAY = 400;
/** Хвост по умолчанию (спец. §6.6). */
const DEFAULT_TAIL = 500;

type Status = "connecting" | "streaming" | "reconnecting" | "ended" | "error";

/**
 * Логи контейнера (спец. §6.6): контейнер, предыдущий запуск, хвост или начало, follow, фильтр
 * (подстрока или regex — применяется в Rust), время строк, буфер до `maxLines` (100 000) строк,
 * сохранение в файл. Счётчики и состояние приходят из Rust и без новых строк; если буфер переполнен,
 * сколько строк с начала отброшено, написано над списком.
 */
export function LogsView({
  connectionId,
  namespace,
  pod,
  containers,
  maxLines = MAX_LINES,
}: {
  connectionId: string;
  namespace: string;
  pod: string;
  containers: { name: string; init: boolean }[];
  /** Размер буфера строк; меняется только в тестах. */
  maxLines?: number;
}) {
  const { t } = useTranslation();
  const followWhy = useId();
  const epoch = useEpoch(connectionId);
  const firstMain = containers.find((c) => !c.init) ?? containers[0];
  const [container, setContainer] = useState(firstMain?.name ?? "");
  const [previous, setPrevious] = useState(false);
  const [tail, setTail] = useState(String(DEFAULT_TAIL));
  const [since, setSince] = useState(0);
  const [follow, setFollow] = useState(true);
  const [timestamps, setTimestamps] = useState(false);
  const [text, setText] = useState("");
  const [regex, setRegex] = useState(false);
  const [caseSensitive, setCaseSensitive] = useState(false);
  const filterText = useDebouncedValue(text, TYPING_DELAY);
  const tailText = useDebouncedValue(tail, TYPING_DELAY);
  const [lines, setLines] = useState<LogLine[]>([]);
  const [counters, setCounters] = useState({ shown: 0, scanned: 0 });
  const [status, setStatus] = useState<Status>("connecting");
  const [error, setError] = useState<AppError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<unknown>(null);
  const [saving, setSaving] = useState(false);
  // «Повторить» после обрыва: меняет зависимость эффекта и начинает сессию заново.
  const [restart, setRestart] = useState(0);
  const paramsFor = (filterValue: string, tailValue: string): LogParams => {
    const tailLines = Number.parseInt(tailValue, 10);
    return {
      connectionId,
      namespace,
      pod,
      container: container || null,
      previous,
      // Предыдущий запуск контейнера уже завершён — следить не за чем.
      follow: follow && !previous,
      tailLines: Number.isFinite(tailLines) && tailLines > 0 ? tailLines : null,
      sinceSeconds: since || null,
      filter: filterValue ? { text: filterValue, regex, caseSensitive } : null,
    };
  };
  // Сессия идёт с отложенными фильтром и хвостом, сохранение — с тем, что набрано сейчас.
  const paramsKey = JSON.stringify(paramsFor(filterText, tailText));

  const parentRef = useRef<HTMLDivElement>(null);
  // Внизу списка — следим за новыми строками; прокрутили вверх — не мешаем читать.
  const atBottom = useRef(true);

  useEffect(() => {
    const p = JSON.parse(paramsKey) as LogParams;
    let active = true;
    let sessionId: string | null = null;
    // Новая сессия начинается с конца списка, даже если предыдущую прокрутили вверх.
    atBottom.current = true;
    setLines([]);
    setCounters({ shown: 0, scanned: 0 });
    setStatus("connecting");
    setError(null);
    api
      .startLogs(p, (e) => {
        if (!active) return;
        switch (e.type) {
          case "lines":
            // Пакет может быть пустым: счётчики без новых строк или поток снова открылся.
            setLines((b) => appendLines(b, e.lines, maxLines));
            setCounters({ shown: e.shown, scanned: e.scanned });
            setStatus("streaming");
            break;
          case "reconnecting":
            setStatus("reconnecting");
            break;
          case "end":
            setCounters({ shown: e.shown, scanned: e.scanned });
            setStatus("ended");
            break;
          case "error":
            setError(e.error);
            setStatus("error");
            break;
        }
      })
      .then((id) => {
        if (!active) {
          api.stopSession(id).catch(() => undefined);
          return;
        }
        sessionId = id;
        // Сессия есть: тихий контейнер или фильтр, под который ничего не подходит, — тоже поток.
        // Ошибка могла прийти раньше ответа — её не затираем.
        setStatus((s) => (s === "connecting" ? "streaming" : s));
      })
      .catch((err) => {
        if (!active) return;
        setError(toAppError(err));
        setStatus("error");
      });
    return () => {
      active = false;
      if (sessionId) api.stopSession(sessionId).catch(() => undefined);
    };
  }, [paramsKey, epoch, restart, maxLines]);

  const virtualizer = useVirtualizer({ count: lines.length, getScrollElement: () => parentRef.current, estimateSize: () => ROW_HEIGHT, overscan: 40 });
  useEffect(() => {
    if (atBottom.current && lines.length > 0) virtualizer.scrollToIndex(lines.length - 1);
  }, [lines.length, virtualizer]);

  const onSave = async () => {
    setNotice(null);
    setSaveError(null);
    setSaving(true);
    try {
      const path = await save({ defaultPath: `${pod}-${container}.log`, filters: [{ name: "Log", extensions: ["log", "txt"] }] });
      // Диалог закрыли без выбора — это не ошибка и не сохранение.
      if (!path) return;
      const n = await api.saveLogs({ ...paramsFor(text, tail), follow: false }, path, timestamps);
      setNotice(t("logs.saved", { count: n, path }));
    } catch (e) {
      setSaveError(e);
    } finally {
      setSaving(false);
    }
  };

  // Сколько подошедших строк уже не в окне: Rust считает сессию целиком, буфер держит последние.
  const dropped = Math.max(0, counters.shown - lines.length);
  const followOff = t("logs.followPrevious");

  return (
    <div className="section logs">
      <div className="toolbar">
        <label className="inline">
          {t("logs.container")}
          <select value={container} onChange={(e) => setContainer(e.target.value)}>
            {containers.map((c) => (
              <option key={c.name} value={c.name}>
                {c.init ? `${c.name} (init)` : c.name}
              </option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="checkbox" checked={previous} onChange={(e) => setPrevious(e.target.checked)} />
          {t("logs.previous")}
        </label>
        <label className="inline">
          {t("logs.tail")}
          <input className="narrow" inputMode="numeric" value={tail} onChange={(e) => setTail(e.target.value)} />
        </label>
        <label className="inline">
          {t("logs.since")}
          <select value={since} onChange={(e) => setSince(Number(e.target.value))}>
            {SINCE.map((s) => (
              <option key={s} value={s}>
                {s === 0 ? t("logs.sinceAll") : s < 3600 ? t("logs.sinceMinutes", { value: s / 60 }) : t("logs.sinceHours", { value: s / 3600 })}
              </option>
            ))}
          </select>
        </label>
        <label className="inline" title={previous ? followOff : undefined}>
          <input
            type="checkbox"
            checked={follow && !previous}
            disabled={previous}
            title={previous ? followOff : undefined}
            aria-describedby={previous ? followWhy : undefined}
            onChange={(e) => setFollow(e.target.checked)}
          />
          {t("logs.follow")}
        </label>
        {previous && (
          <span id={followWhy} hidden>
            {followOff}
          </span>
        )}
        <label className="inline">
          <input type="checkbox" checked={timestamps} onChange={(e) => setTimestamps(e.target.checked)} />
          {t("logs.timestamps")}
        </label>
      </div>
      <div className="toolbar">
        <input aria-label={t("logs.filter")} placeholder={t("logs.filter")} value={text} onChange={(e) => setText(e.target.value)} />
        <label className="inline">
          <input type="checkbox" checked={regex} onChange={(e) => setRegex(e.target.checked)} />
          regex
        </label>
        <Hint k="hints.logs.filter" label={t("logs.filter")} />
        <label className="inline">
          <input type="checkbox" checked={caseSensitive} onChange={(e) => setCaseSensitive(e.target.checked)} />
          {t("logs.caseSensitive")}
        </label>
        <span className="muted">{t("logs.counters", { shown: counters.shown, scanned: counters.scanned })}</span>
        <span role="status" className={status === "reconnecting" ? "warn" : "muted"}>
          {t(`logs.status.${status}`)}
        </span>
        <button type="button" disabled={saving} aria-busy={saving} onClick={() => void onSave()}>
          {t("logs.save")}
        </button>
      </div>
      <div role="status" className="muted">
        {notice}
      </div>
      {saveError !== null && <ErrorBanner error={saveError} />}
      {error && <ReadError error={error} connectionId={connectionId} onRetry={error.retryable ? () => setRestart((n) => n + 1) : undefined} />}
      {dropped > 0 && <div className="muted">{t("logs.dropped", { count: dropped, max: maxLines })}</div>}
      {/* Прокручивается с клавиатуры (tabIndex); живая область выключена: строки приходят и уходят из окна пачками. */}
      <div
        className="vlist log-lines"
        ref={parentRef}
        role="log"
        tabIndex={0}
        aria-live="off"
        aria-label={t("logs.list", { name: [pod, container].filter(Boolean).join(" / ") })}
        onScroll={(e) => {
          const el = e.currentTarget;
          atBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < ROW_HEIGHT * 2;
        }}
      >
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {virtualizer.getVirtualItems().map((item) => {
            const l = lines[item.index];
            return (
              <div key={item.index} className="log-line" style={{ top: item.start, height: ROW_HEIGHT }}>
                {timestamps && l.ts && <span className="muted">{l.ts} </span>}
                {l.text}
                {l.truncated && <span className="muted"> {t("logs.truncated")}</span>}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
