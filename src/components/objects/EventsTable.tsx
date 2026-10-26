import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import type { EventRow } from "../../api/types";
import { formatAge } from "../../lib/age";
import { eventKey } from "../../lib/useWatchList";
import { type Column, VirtualTable } from "../common/VirtualTable";

/** Таблица событий (спец. §6.3) с фильтрами по типу, причине и объекту; новые — сверху. */
export function EventsTable({ rows }: { rows: EventRow[] }) {
  const { t } = useTranslation();
  const [type, setType] = useState("");
  const [reason, setReason] = useState("");
  const [object, setObject] = useState("");
  const reasons = useMemo(() => [...new Set(rows.map((e) => e.reason))].sort(), [rows]);
  const shown = useMemo(() => {
    const o = object.trim().toLowerCase();
    return rows
      .filter((e) => (!type || e.type === type) && (!reason || e.reason === reason) && (!o || `${e.object.kind}/${e.object.name}`.toLowerCase().includes(o)))
      .sort((a, b) => (b.lastSeen ?? 0) - (a.lastSeen ?? 0));
  }, [rows, type, reason, object]);
  const columns: Column<EventRow>[] = [
    { key: "last", title: t("objects.events.lastSeen"), width: "80px", render: (e) => formatAge(e.lastSeen) },
    { key: "type", title: t("objects.events.type"), width: "80px", render: (e) => <span className={e.type === "Warning" ? "problem" : undefined}>{e.type}</span> },
    { key: "reason", title: t("objects.events.reason"), width: "minmax(110px, 1fr)", render: (e) => e.reason },
    { key: "object", title: t("objects.events.object"), width: "minmax(160px, 1.5fr)", render: (e) => `${e.object.kind}/${e.object.name}` },
    { key: "message", title: t("objects.events.message"), width: "minmax(240px, 4fr)", render: (e) => <span title={e.message}>{e.message}</span> },
    { key: "count", title: t("objects.events.count"), width: "60px", render: (e) => e.count },
  ];
  return (
    <>
      <div className="toolbar">
        <label className="inline">
          {t("objects.events.type")}
          <select value={type} onChange={(e) => setType(e.target.value)}>
            <option value="">{t("objects.events.all")}</option>
            <option value="Warning">Warning</option>
            <option value="Normal">Normal</option>
          </select>
        </label>
        <label className="inline">
          {t("objects.events.reason")}
          <select value={reason} onChange={(e) => setReason(e.target.value)}>
            <option value="">{t("objects.events.all")}</option>
            {reasons.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </label>
        <input aria-label={t("objects.events.object")} placeholder={t("objects.events.objectFilter")} value={object} onChange={(e) => setObject(e.target.value)} />
      </div>
      <VirtualTable label={t("objects.events.title")} rows={shown} columns={columns} rowKey={eventKey} />
    </>
  );
}
