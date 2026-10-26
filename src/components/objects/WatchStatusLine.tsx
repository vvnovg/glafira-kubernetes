import { useTranslation } from "react-i18next";
import { tr } from "../../i18n/text";
import type { WatchState } from "../../lib/useWatchList";

/** Как обновляется список: загрузка, опрос без прав на watch, временная ошибка (watcher повторит сам). */
export function WatchStatusLine({ state }: { state: WatchState<unknown> }) {
  const { t } = useTranslation();
  if (state.status === "connecting") return <span className="muted">{t("common.loading")}</span>;
  if (state.status === "error") return null;
  return (
    <>
      {state.status === "polling" && <span className="muted">{t("objects.watch.polling")}</span>}
      {state.error && <span className="warn">{tr(state.error)}</span>}
    </>
  );
}
