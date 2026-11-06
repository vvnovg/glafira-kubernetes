import { useTranslation } from "react-i18next";
import { Icon, type IconName } from "./Icon";

export type MarkState = "ok" | "warn" | "error" | "progress";

const ICONS: Record<MarkState, IconName> = { ok: "check", warn: "warning", error: "error", progress: "hourglass" };
const LABEL_KEYS: Record<MarkState, string> = {
  ok: "common.state.ok",
  warn: "common.state.warn",
  error: "common.state.error",
  progress: "common.state.progress",
};

/**
 * Значок состояния вместо emoji «✅ ⚠ ❌ ⏳» (спец. U1 §5): цвет по состоянию, для экранного диктора — подпись
 * (`label` или общая из словаря), как у прежних символов с ролью изображения.
 */
export function StateMark({ state, label }: { state: MarkState; label?: string }) {
  const { t } = useTranslation();
  return (
    <span className={`state-mark state-${state}`} role="img" aria-label={label ?? t(LABEL_KEYS[state])}>
      <Icon name={ICONS[state]} size={14} />
    </span>
  );
}
