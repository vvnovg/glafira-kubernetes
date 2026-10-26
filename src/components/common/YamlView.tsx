import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { api } from "../../api/client";
import type { YamlKind } from "../../api/types";
import { ReadError } from "./ReadError";

/** YAML объекта только для чтения, без `managedFields` (спец. §6.4); копирование в буфер. */
export function YamlView({ connectionId, namespace, kind, name }: { connectionId: string; namespace: string; kind: YamlKind; name: string }) {
  const { t } = useTranslation();
  const q = useQuery({ queryKey: ["yaml", connectionId, namespace, kind, name], queryFn: () => api.getYaml(connectionId, namespace, kind, name) });
  const [copied, setCopied] = useState(false);
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
            if (!q.data) return;
            navigator.clipboard.writeText(q.data).then(
              () => setCopied(true),
              () => setCopied(false),
            );
          }}
        >
          {t("common.copy")}
        </button>
        {copied && <span className="muted">{t("common.copied")}</span>}
      </div>
      {q.isPending && <p className="muted">{t("common.loading")}</p>}
      {q.error && <ReadError error={q.error} connectionId={connectionId} onRetry={() => void q.refetch()} />}
      {q.data && <pre className="yaml">{q.data}</pre>}
    </div>
  );
}
