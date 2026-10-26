import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { toAppError } from "../../api/client";
import { needsSignIn } from "../../lib/signIn";
import { useWorkspace } from "../../state/workspace";
import { reconnect, sessionMutationKey } from "../cluster/SessionActions";
import { ErrorBanner } from "./ErrorBanner";

/**
 * Ошибка чтения на вкладке объекта. Сессия истекла — «Войти снова» подключается заново и открывает вкладку
 * подключения: удалось (exec-плагин выдал новый токен) — вкладки перечитываются сами (`afterSignIn`), нет — ошибка
 * подключения попадает в запрос вкладки, и там видны способы войти (`SessionActions`). Объект удалён — пояснение;
 * нет прав — без «Повторить»; прочее — баннер с «Повторить».
 */
export function ReadError({ error, connectionId, onRetry }: { error: unknown; connectionId: string; onRetry?: () => void }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const queryClient = useQueryClient();
  // Тот же ключ, что у входа на вкладке подключения: пока идёт подключение, второе рядом не запускается.
  const signIn = useMutation({ mutationKey: sessionMutationKey(connectionId), mutationFn: () => reconnect(queryClient, connectionId) });
  const e = toAppError(error);
  if (needsSignIn(e))
    return (
      <div className="panel">
        <ErrorBanner error={e} />
        <div className="toolbar">
          <button
            type="button"
            className="primary"
            disabled={signIn.isPending}
            onClick={() => {
              signIn.mutate();
              open({ kind: "cluster", connectionId });
            }}
          >
            {t("objects.signInAgain")}
          </button>
        </div>
      </div>
    );
  if (e.code === "NOT_FOUND") return <p className="empty">{t("objects.gone")}</p>;
  return <ErrorBanner error={e} onRetry={e.code === "FORBIDDEN" ? undefined : onRetry} />;
}
