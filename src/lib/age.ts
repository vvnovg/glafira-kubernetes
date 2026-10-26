import { currentLocale, i18n } from "../i18n";

/** Возраст объекта коротко, как в kubectl: «45 с», «12 мин», «5 ч», «3 д»; нет времени — «—». */
export function formatAge(createdSec: number | null | undefined, nowMs = Date.now()): string {
  if (createdSec == null) return "—";
  const s = Math.max(0, Math.floor(nowMs / 1000 - createdSec));
  if (s < 120) return i18n.t("common.age.seconds", { n: s });
  if (s < 7200) return i18n.t("common.age.minutes", { n: Math.floor(s / 60) });
  if (s < 172_800) return i18n.t("common.age.hours", { n: Math.floor(s / 3600) });
  return i18n.t("common.age.days", { n: Math.floor(s / 86_400) });
}

/** Дата и время (секунды Unix) в формате языка интерфейса; нет времени — «—». */
export function formatDateTime(sec: number | null | undefined): string {
  if (sec == null) return "—";
  return new Date(sec * 1000).toLocaleString(currentLocale());
}
