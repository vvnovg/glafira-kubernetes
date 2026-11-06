import { useTranslation } from "react-i18next";
import { useMcpStatus } from "../../lib/useMcpStatus";
import { useWorkspace } from "../../state/workspace";
import { Icon } from "../common/Icon";
import { TreeRow } from "../connections/tree/TreeRow";

/** «LLM: <клиент>» внизу боковой панели, пока есть сессии MCP; точка подсвечивается во время вызова. */
export function McpIndicator() {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const { data: s } = useMcpStatus();
  if (!s || s.sessions.length === 0) return null;
  const names = [...new Set(s.sessions.map((x) => x.clientName ?? t("mcp.unknownClient")))];
  const active = s.sessions.some((x) => x.active);
  const label = names.length === 1 ? t("mcp.indicator.one", { client: names[0] }) : t("mcp.indicator.many", { n: names.length });
  return (
    <TreeRow
      chevron="none"
      className={active ? "mcp-indicator active" : "mcp-indicator"}
      icon={<Icon name="dot" size={12} />}
      name={label}
      title={t("mcp.indicator.title")}
      onActivate={() => open({ kind: "settings" })}
    />
  );
}
