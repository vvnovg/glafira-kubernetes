import { useTranslation } from "react-i18next";
import type { ConditionView } from "../../api/types";
import { formatAge } from "../../lib/age";

/** Условия объекта (`status.conditions`): вид, статус, причина, сообщение, с какого времени. */
export function Conditions({ conditions }: { conditions: ConditionView[] }) {
  const { t } = useTranslation();
  if (conditions.length === 0) return <p className="muted">—</p>;
  return (
    <table>
      <thead>
        <tr>
          <th>{t("objects.conditions.type")}</th>
          <th>{t("objects.conditions.status")}</th>
          <th>{t("objects.conditions.reason")}</th>
          <th>{t("objects.conditions.message")}</th>
          <th>{t("objects.conditions.since")}</th>
        </tr>
      </thead>
      <tbody>
        {conditions.map((c) => (
          <tr key={c.type}>
            <td>{c.type}</td>
            <td className={c.status === "True" ? undefined : "muted"}>{c.status}</td>
            <td>{c.reason ?? ""}</td>
            <td>{c.message ?? ""}</td>
            <td>{formatAge(c.lastTransition ?? null)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
