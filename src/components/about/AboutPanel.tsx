import { lazy, Suspense, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../api/client";
import type { UpdateCheck } from "../../api/types";
import { tr } from "../../i18n/text";
import { openLicense, openSupport } from "../../lib/links";
import { ChunkBoundary } from "../common/ChunkBoundary";
import { ErrorBanner } from "../common/ErrorBanner";

// Заметки рисует react-markdown — отдельным чанком при первом показе, как справку.
const ReleaseNotes = lazy(() => import("./ReleaseNotes").then((m) => ({ default: m.ReleaseNotes })));

/** Заметки на языке интерфейса, иначе английские, иначе `notes`. */
function notesFor(update: Extract<UpdateCheck, { status: "available" }>, language: string): string | null {
  return update.notesI18n[language] ?? update.notesI18n.en ?? update.notes;
}

export function AboutPanel() {
  const { t } = useTranslation();
  const info = useQuery({ queryKey: ["app-info"], queryFn: api.getAppInfo });
  const policy = useQuery({ queryKey: ["policy"], queryFn: api.getPolicyStatus });

  if (info.error) return <ErrorBanner error={info.error} onRetry={() => info.refetch()} />;
  const i = info.data;
  if (!i) return <p className="empty">{t("common.loading")}</p>;
  return (
    <div className="page">
      <div className="toolbar">
        <h2>{i.name}</h2>
      </div>
      <p className="about-summary">{t("about.summary")}</p>
      <dl className="stats">
        <div><dt>{t("about.version")}</dt><dd>{i.version}</dd></div>
        <div><dt>{t("about.build")}</dt><dd>{i.commit}</dd></div>
        <div><dt>{t("about.system")}</dt><dd>{i.os} / {i.arch}</dd></div>
        <div>
          <dt>{t("about.license")}</dt>
          <dd>
            <button type="button" className="link" onClick={openLicense}>
              Apache 2.0
            </button>
          </dd>
        </div>
        <div><dt>{t("about.policy")}</dt><dd>{policy.data ? (policy.data.managed ? t("about.policySet") : t("about.policyNone")) : "…"}</dd></div>
      </dl>
      {policy.data?.errors.map((e, n) => (
        <p key={n} className="warn">
          {t("about.policyError", { error: tr(e) })}
        </p>
      ))}
      <p>
        <button type="button" className="link" onClick={openSupport}>
          {t("support.title")}
        </button>{" "}
        <span className="muted">{t("support.hint")}</span>
      </p>
      <p className="muted">{t("about.dataDir")} <code>{i.dataDir}</code></p>
      <p className="muted">{t("about.logDir")} <code>{i.logDir}</code></p>
      <UpdatesSection />
      <p className="muted">{t("about.trademarks")}</p>
    </div>
  );
}

function UpdatesSection() {
  const { t, i18n } = useTranslation();
  const check = useMutation({ mutationFn: api.checkUpdate });
  const [confirm, setConfirm] = useState<string | null>(null);
  const install = useMutation({
    mutationFn: (force: boolean) => api.installUpdate(force),
    onMutate: () => setConfirm(null),
    onError: (e) => {
      const err = toAppError(e);
      if (err.code === "CONFIRMATION_REQUIRED") setConfirm(err.message);
    },
  });
  const r = check.data;
  const notes = r?.status === "available" ? notesFor(r, i18n.language) : null;
  const installError = install.error && toAppError(install.error).code !== "CONFIRMATION_REQUIRED" ? install.error : null;
  return (
    <section className="panel">
      <h3>{t("about.updates.title")}</h3>
      <div className="toolbar">
        <button
          type="button"
          disabled={check.isPending || install.isPending}
          onClick={() => {
            install.reset();
            setConfirm(null);
            check.mutate();
          }}
        >
          {t("about.updates.check")}
        </button>
      </div>
      {check.error && <ErrorBanner error={check.error} onRetry={() => check.mutate()} />}
      {r?.status === "disabled" && <p className="muted">{tr(r.reason)}</p>}
      {r?.status === "upToDate" && <p role="status">{t("about.updates.upToDate")}</p>}
      {r?.status === "available" && (
        <>
          <p role="status">{t("about.updates.available", { version: r.version })}</p>
          {notes && (
            <section className="release-notes" aria-label={t("about.updates.notesAria", { version: r.version })}>
              <ChunkBoundary fallback={<pre>{notes}</pre>}>
                <Suspense fallback={<p className="muted">{t("common.loading")}</p>}>
                  <ReleaseNotes markdown={notes} />
                </Suspense>
              </ChunkBoundary>
            </section>
          )}
          {confirm ? (
            <div className="confirm" role="alertdialog" aria-label={t("about.updates.confirmAria")}>
              <p>{confirm}</p>
              <div className="toolbar">
                <button type="button" disabled={install.isPending} onClick={() => install.mutate(true)}>
                  {t("about.updates.stopAndUpdate")}
                </button>
                <button type="button" onClick={() => setConfirm(null)}>
                  {t("common.cancel")}
                </button>
              </div>
            </div>
          ) : (
            <div className="toolbar">
              <button type="button" disabled={install.isPending} onClick={() => install.mutate(false)}>
                {t("about.updates.install")}
              </button>
            </div>
          )}
          {install.isPending && <p className="muted">{t("about.updates.installing")}</p>}
          {installError && <ErrorBanner error={installError} />}
        </>
      )}
    </section>
  );
}
