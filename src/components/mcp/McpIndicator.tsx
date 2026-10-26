import { useTranslation } from "react-i18next";
import { useMcpStatus } from "../../lib/useMcpStatus";
import { useWorkspace } from "../../state/workspace";

/** «LLM: <клиент>» внизу боковой панели, пока есть сессии MCP; подсвечивается во время вызова. */
export function McpIndicator() {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const { data: s } = useMcpStatus();
  if (!s || s.sessions.length === 0) return null;
  const names = [...new Set(s.sessions.map((x) => x.clientName ?? t("mcp.unknownClient")))];
  const active = s.sessions.some((x) => x.active);
  const label = names.length === 1 ? t("mcp.indicator.one", { client: names[0] }) : t("mcp.indicator.many", { n: names.length });
  return (
    <button
      type="button"
      className={active ? "link mcp-indicator active" : "link mcp-indicator"}
      title={t("mcp.indicator.title")}
      onClick={() => open({ kind: "settings" })}
    >
      {label}
    </button>
  );
}
