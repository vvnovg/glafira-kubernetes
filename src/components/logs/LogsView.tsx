import { useVirtualizer } from "@tanstack/react-virtual";
import { save } from "@tauri-apps/plugin-dialog";
import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { AppError, LogLine, LogParams } from "../../api/types";
import { useDebouncedValue } from "../../lib/useDebouncedValue";
import { useEpoch } from "../../state/connectionEpoch";
import { ErrorBanner } from "../common/ErrorBanner";
import { Hint } from "../common/Hint";
import { ReadError } from "../common/ReadError";
import { appendLines } from "./buffer";

const ROW_HEIGHT = 18;
/** Начало лога: без ограничения или последние 5 мин, 1 ч, 6 ч, 24 ч (секунды). */
const SINCE = [0, 300, 3600, 21_600, 86_400];
/** Хвост по умолчанию (спец. §6.6). */
const DEFAULT_TAIL = 500;

type Status = "connecting" | "streaming" | "reconnecting" | "ended" | "error";

/**
 * Логи контейнера (спец. §6.6): контейнер, предыдущий запуск, хвост или начало, follow, фильтр
 * (подстрока или regex — применяется в Rust), время строк, буфер до 100 000 строк, сохранение в файл.
 */
export function LogsView({
  connectionId,
  namespace,
  pod,
  containers,
}: {
  connectionId: string;
  namespace: string;
  pod: string;
  containers: { name: string; init: boolean }[];
}) {
  const { t } = useTranslation();
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
  const filterText = useDebouncedValue(text, 400);
  const [lines, setLines] = useState<LogLine[]>([]);
  const [counters, setCounters] = useState({ shown: 0, scanned: 0 });
  const [status, setStatus] = useState<Status>("connecting");
  const [error, setError] = useState<AppError | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [saveError, setSaveError] = useState<unknown>(null);
  const tailLines = Number.parseInt(tail, 10);
  const params: LogParams = {
    connectionId,
    namespace,
    pod,
    container: container || null,
    previous,
    // Предыдущий запуск контейнера уже завершён — следить не за чем.
    follow: follow && !previous,
    tailLines: Number.isFinite(tailLines) && tailLines > 0 ? tailLines : null,
    sinceSeconds: since || null,
    filter: filterText ? { text: filterText, regex, caseSensitive } : null,
  };
  const paramsKey = JSON.stringify(params);

  useEffect(() => {
    const p = JSON.parse(paramsKey) as LogParams;
    let active = true;
    let sessionId: string | null = null;
    setLines([]);
    setCounters({ shown: 0, scanned: 0 });
    setStatus("connecting");
    setError(null);
    api
      .startLogs(p, (e) => {
        if (!active) return;
        switch (e.type) {
          case "lines":
            setLines((b) => appendLines(b, e.lines));
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
        if (active) sessionId = id;
        else api.stopSession(id).catch(() => undefined);
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
  }, [paramsKey, epoch]);

  const parentRef = useRef<HTMLDivElement>(null);
  // Внизу списка — следим за новыми строками; прокрутили вверх — не мешаем читать.
  const atBottom = useRef(true);
  const virtualizer = useVirtualizer({ count: lines.length, getScrollElement: () => parentRef.current, estimateSize: () => ROW_HEIGHT, overscan: 40 });
  useEffect(() => {
    if (atBottom.current && lines.length > 0) virtualizer.scrollToIndex(lines.length - 1);
  }, [lines.length, virtualizer]);

  const onSave = async () => {
    setNotice(null);
    setSaveError(null);
    try {
      const path = await save({ defaultPath: `${pod}-${container}.log`, filters: [{ name: "Log", extensions: ["log", "txt"] }] });
      if (!path) return;
      const n = await api.saveLogs({ ...params, follow: false }, path, timestamps);
      setNotice(t("logs.saved", { count: n, path }));
    } catch (e) {
      setSaveError(e);
    }
  };

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
                {s === 0 ? t("logs.sinceAll") : t("logs.sinceLast", { minutes: s / 60 })}
              </option>
            ))}
          </select>
        </label>
        <label className="inline">
          <input type="checkbox" checked={follow && !previous} disabled={previous} onChange={(e) => setFollow(e.target.checked)} />
          {t("logs.follow")}
        </label>
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
        <span className={status === "reconnecting" ? "warn" : "muted"}>{t(`logs.status.${status}`)}</span>
        <button type="button" onClick={() => void onSave()}>
          {t("logs.save")}
        </button>
      </div>
      {notice && <p className="muted">{notice}</p>}
      {saveError !== null && <ErrorBanner error={saveError} />}
      {error && <ReadError error={error} connectionId={connectionId} />}
      <div
        className="vlist log-lines"
        ref={parentRef}
        role="log"
        aria-label={t("logs.title")}
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
