import { useTranslation } from "react-i18next";
import { toAppError } from "../../api/client";
import { needsSignIn } from "../../lib/signIn";
import { useWorkspace } from "../../state/workspace";
import { ErrorBanner } from "./ErrorBanner";

/**
 * Ошибка чтения на вкладке объекта. Сессия истекла — «Войти снова» открывает вкладку подключения
 * (там вход), после входа вкладки перечитываются сами; объект удалён — пояснение; нет прав — без
 * «Повторить»; прочее — баннер с «Повторить».
 */
export function ReadError({ error, connectionId, onRetry }: { error: unknown; connectionId: string; onRetry?: () => void }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const e = toAppError(error);
  if (needsSignIn(e))
    return (
      <div className="panel">
        <ErrorBanner error={e} />
        <div className="toolbar">
          <button type="button" className="primary" onClick={() => open({ kind: "cluster", connectionId })}>
            {t("objects.signInAgain")}
          </button>
        </div>
      </div>
    );
  if (e.code === "NOT_FOUND") return <p className="empty">{t("objects.gone")}</p>;
  return <ErrorBanner error={e} onRetry={e.code === "FORBIDDEN" ? undefined : onRetry} />;
}
