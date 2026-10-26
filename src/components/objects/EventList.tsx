import { useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { EventObject, EventRow } from "../../api/types";
import { useRefetchInterval } from "../../lib/tabActive";
import { eventKey, useWatchList } from "../../lib/useWatchList";
import { ReadError } from "../common/ReadError";
import { RefreshControls, useRefreshInterval } from "../common/RefreshControls";
import { TruncatedNote } from "../common/TruncatedNote";
import { EventsTable } from "./EventsTable";
import { WatchStatusLine } from "./WatchStatusLine";

/** События всего проекта: обновляются сами (watch или опрос). */
export function ProjectEvents({ connectionId, namespace }: { connectionId: string; namespace: string }) {
  const watch = useWatchList<EventRow>({ connectionId, namespace, kind: "events" }, eventKey);
  return (
    <div className="section">
      <WatchStatusLine state={watch} />
      {watch.status === "error" && watch.error && <ReadError error={watch.error} connectionId={connectionId} />}
      <TruncatedNote truncated={watch.truncated} />
      <EventsTable rows={watch.rows} />
    </div>
  );
}

/** События объекта (у workload'а — вместе с его подами): «Обновить» и автообновление. */
export function ObjectEvents({ connectionId, namespace, object }: { connectionId: string; namespace: string; object: EventObject }) {
  const { t } = useTranslation();
  const [interval, setInterval] = useRefreshInterval();
  const refetchInterval = useRefetchInterval(interval);
  const q = useQuery({
    queryKey: ["events", connectionId, namespace, object],
    queryFn: () => api.listEvents(connectionId, namespace, object),
    refetchInterval,
  });
  return (
    <div className="section">
      <RefreshControls interval={interval} onInterval={setInterval} onRefresh={() => void q.refetch()} busy={q.isFetching} />
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      <TruncatedNote truncated={q.data?.truncated} />
      {q.data && <EventsTable rows={q.data.items} />}
    </div>
  );
}
