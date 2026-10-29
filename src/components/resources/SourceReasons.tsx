import { useQuery } from "@tanstack/react-query";
import { api, toAppError } from "../../api/client";
import type { MetricsRejected } from "../../api/types";
import { tr } from "../../i18n/text";
import { ErrorBanner } from "../common/ErrorBanner";
import { ReadError } from "../common/ReadError";

/**
 * Почему остальные источники метрик не подошли (спец. §7.1): строка на источник. Текст причины из Rust уже
 * начинается с имени источника («Thanos Querier …: нет прав на мониторинг кластера …»), поэтому его не повторяем.
 */
export function SourceReasons({ rejected }: { rejected: MetricsRejected[] }) {
  if (rejected.length === 0) return null;
  return (
    <ul className="muted">
      {rejected.map((r) => (
        <li key={`${r.source.kind}:${r.source.location}`}>{tr(r.message)}</li>
      ))}
    </ul>
  );
}

/** Нет ни одного источника: причины берутся из выбора источника подключения, а не из английских подробностей ошибки. */
function Unavailable({ error, connectionId, at, onRetry }: { error: unknown; connectionId: string; at: number; onRetry?: () => void }) {
  // Выбор лежит в подключении (`refresh = false` его не пересчитывает); ключ с временем ошибки перечитывает причины
  // после «Повторить», который выбирает источник заново.
  const status = useQuery({ queryKey: ["metricsSource", connectionId, at], queryFn: () => api.getMetricsSourceStatus(connectionId, false) });
  const e = toAppError(error);
  return (
    <>
      {/* Английские подробности остаются только тогда, когда переведённые причины прочитать не удалось. */}
      <ErrorBanner error={status.isError ? e : { ...e, details: undefined }} onRetry={onRetry} />
      <SourceReasons rejected={status.data?.rejected ?? []} />
    </>
  );
}

/**
 * Ошибка вкладки «Ресурсы»: `METRICS_UNAVAILABLE` — с переведёнными причинами отказа каждого источника, прочее —
 * как на других вкладках объекта. `at` — время ошибки запроса (`errorUpdatedAt`).
 */
export function ResourcesError({ error, connectionId, at, onRetry }: { error: unknown; connectionId: string; at: number; onRetry?: () => void }) {
  if (toAppError(error).code === "METRICS_UNAVAILABLE") return <Unavailable error={error} connectionId={connectionId} at={at} onRetry={onRetry} />;
  return <ReadError error={error} connectionId={connectionId} onRetry={onRetry} />;
}
