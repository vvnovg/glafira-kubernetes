import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import type { ContainerSpecView, ContainerStatusView, StateView } from "../../api/types";
import { formatDateTime } from "../../lib/age";

const isStatus = (c: ContainerSpecView | ContainerStatusView): c is ContainerStatusView => "restarts" in c;

/** Состояние контейнера словами: «работает с …», «ждёт: CrashLoopBackOff», «завершён: Error, код 1». */
function StateText({ s }: { s: StateView }) {
  const { t } = useTranslation();
  const reason = [s.reason, s.message].filter(Boolean).join(" — ");
  switch (s.state) {
    case "running":
      return <>{t("objects.container.running", { since: formatDateTime(s.started) })}</>;
    case "waiting":
      return <span className="problem">{t("objects.container.waiting", { reason: reason || "—" })}</span>;
    case "terminated":
      return (
        <span className={s.exitCode ? "problem" : undefined}>
          {t("objects.container.terminated", { reason: reason || "—", code: s.exitCode ?? "—", at: formatDateTime(s.finished) })}
        </span>
      );
  }
}

/** Контейнер шаблона или пода (спец. §6.5): образ, состояние, ресурсы, пробы, переменные окружения (имена и источники, без значений). */
export function ContainerCard({ c }: { c: ContainerSpecView | ContainerStatusView }) {
  const { t } = useTranslation();
  const resource = (r: "cpu" | "memory") => `${c.requests[r] ?? "—"} / ${c.limits[r] ?? "—"}`;
  return (
    <div className="card">
      <h4>
        {c.name} {c.init && <span className="muted">({t("objects.container.init")})</span>}
      </h4>
      <dl className="facts">
        <dt>{t("objects.container.image")}</dt>
        <dd>
          <code>{c.image}</code>
          {isStatus(c) && c.imageId && (
            <>
              <br />
              <code className="muted">{c.imageId}</code>
            </>
          )}
        </dd>
        {isStatus(c) && (
          <>
            <dt>{t("objects.container.state")}</dt>
            <dd>{c.state ? <StateText s={c.state} /> : "—"}</dd>
            {c.lastState && (
              <>
                <dt>{t("objects.container.lastState")}</dt>
                <dd>
                  <StateText s={c.lastState} />
                </dd>
              </>
            )}
            <dt>{t("objects.container.ready")}</dt>
            <dd>{c.ready ? "✅" : "—"}</dd>
            <dt>{t("objects.container.restarts")}</dt>
            <dd>{c.restarts}</dd>
          </>
        )}
        <dt>CPU</dt>
        <dd>{resource("cpu")}</dd>
        <dt>{t("objects.container.memory")}</dt>
        <dd>{resource("memory")}</dd>
        {c.ports.length > 0 && (
          <>
            <dt>{t("objects.container.ports")}</dt>
            <dd>{c.ports.join(", ")}</dd>
          </>
        )}
        {(["liveness", "readiness", "startup"] as const).map(
          (p) =>
            c[p] && (
              <Fragment key={p}>
                <dt>{t(`objects.container.${p}`)}</dt>
                <dd>
                  <code>{c[p]}</code>
                </dd>
              </Fragment>
            ),
        )}
      </dl>
      {c.env.length > 0 && (
        <details>
          <summary>{t("objects.container.env", { count: c.env.length })}</summary>
          <ul className="kv">
            {c.env.map((e) => (
              <li key={`${e.name}:${e.source}:${e.reference ?? ""}`}>
                <code>
                  {e.name} ← {e.source === "literal" ? t("objects.container.literal") : `${e.source} ${e.reference ?? ""}`}
                </code>
              </li>
            ))}
          </ul>
        </details>
      )}
    </div>
  );
}
