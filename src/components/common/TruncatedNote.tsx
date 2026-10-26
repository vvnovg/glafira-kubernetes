import { useTranslation } from "react-i18next";

/**
 * «Показаны первые 10 000»: список прочитан не весь (`truncated` у `Listed`, `WorkloadList`, `ProjectList` и сессии
 * watch), цифры и проблемы по нему — тоже только по прочитанному. Без `truncated` ничего не рисует.
 */
export function TruncatedNote({ truncated, className = "warn" }: { truncated: boolean | undefined; className?: string }) {
  const { t } = useTranslation();
  if (!truncated) return null;
  return (
    <p className={className} role="status">
      {t("common.truncated")}
    </p>
  );
}
