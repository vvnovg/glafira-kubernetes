import { useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { YamlKind } from "../../api/types";
import { ReadError } from "./ReadError";

/** Сколько показывается итог копирования. */
const COPY_NOTICE_MS = 2000;

/** YAML объекта только для чтения, без `managedFields` (спец. §6.4); копирование в буфер с итогом на 2 с. */
export function YamlView({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: YamlKind; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["yaml", connectionId, namespace, kind, name], queryFn: () => api.getYaml(connectionId, namespace, kind, name) });
  // Итог копирования относится к тексту, который копировали: другой текст (обновлённый YAML) его не показывает.
  const [copy, setCopy] = useState<{ text: string; ok: boolean } | null>(null);
  const result = copy && copy.text === q.data ? copy : null;
  useEffect(() => {
    if (!copy) return;
    const timer = setTimeout(() => setCopy(null), COPY_NOTICE_MS);
    return () => clearTimeout(timer);
  }, [copy]);
  const onCopy = async (text: string) => {
    let ok = true;
    try {
      await navigator.clipboard.writeText(text);
    } catch {
      ok = false;
    }
    setCopy({ text, ok });
  };
  return (
    <div className="panel">
      <div className="toolbar">
        <button type="button" disabled={q.isFetching} onClick={() => void q.refetch()}>
          {t("common.refresh")}
        </button>
        <button
          type="button"
          disabled={!q.data}
          onClick={() => {
            if (q.data) void onCopy(q.data);
          }}
        >
          {t("common.copy")}
        </button>
        {result?.ok && (
          <span className="muted" role="status">
            {t("common.copied")}
          </span>
        )}
        {result && !result.ok && (
          <span className="warn" role="alert">
            {t("common.copyFailed")}
          </span>
        )}
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <pre className="yaml">{q.data}</pre>}
    </div>
  );
}
