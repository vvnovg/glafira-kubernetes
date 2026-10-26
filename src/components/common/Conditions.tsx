import { useTranslation } from "react-i18next";
import type { ConditionView } from "../../api/types";
import { formatAge } from "../../lib/age";

/** Условия, у которых проблема — `True`: давление на ноде, нет сети ноды, сбой реплик, неудавшееся задание. У остальных проблема — `False`. */
const TRUE_IS_PROBLEM = /Pressure$|^(NetworkUnavailable|ReplicaFailure|Failed)$/;

/** Класс статуса: проблемное состояние — `problem`, неизвестное — `warn`, нормальное — без класса. */
function statusClass(c: ConditionView): string | undefined {
  if (c.status !== "True" && c.status !== "False") return "warn";
  const bad = TRUE_IS_PROBLEM.test(c.type) ? "True" : "False";
  return c.status === bad ? "problem" : undefined;
}

/** Условия объекта (`status.conditions`): вид, статус, причина, сообщение, с какого времени. Проблемный статус выделен. */
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
            <td className={statusClass(c)}>{c.status}</td>
            <td>{c.reason ?? ""}</td>
            <td>{c.message ?? ""}</td>
            <td>{formatAge(c.lastTransition ?? null)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
