import { useQuery } from "@tanstack/react-query";
import { type HTMLAttributes, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../../api/client";
import type { ConnectionProfile } from "../../../api/types";
import { tr } from "../../../i18n/text";
import { useWorkspace } from "../../../state/workspace";
import { EnvBadge } from "../../common/EnvBadge";
import { TruncatedNote } from "../../common/TruncatedNote";

/** С какого числа проектов показывается фильтр. */
const FILTER_FROM = 8;

/**
 * Узел подключения в дереве: точка цвета, имя, окружение и пункты подключения — «Кластер», «Проекты»
 * (разворачиваются и читаются по щелчку) и «Ноды», если на них есть права. rowProps — атрибуты строки
 * (перетаскивание).
 */
export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [projectsOpen, setProjectsOpen] = useState(false);
  // Сведения о подключении — из вкладки подключения; само дерево не подключается.
  const info = useQuery({ queryKey: ["connection", c.id], queryFn: () => api.connect(c.id, false), enabled: false });

  return (
    <li>
      <div {...rowProps} className={rowProps?.className ? `tree-conn ${rowProps.className}` : "tree-conn"}>
        <span className="dot" style={{ background: c.color }} />
        <span className="tree-name">{c.name}</span>
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
        {c.llmAccess === "metadata" && (
          <span className="llm-badge" title={t("sidebar.llmAccess")}>
            LLM
          </span>
        )}
        <button type="button" className="icon" title={t("sidebar.editConnection")} onClick={() => open({ kind: "connection-form", connectionId: c.id })}>
          ✎
        </button>
      </div>
      <ul>
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "cluster", connectionId: c.id })}>
            {t("sidebar.cluster")}
          </button>
        </li>
        <li>
          <button type="button" className="tree-folder-toggle" aria-expanded={projectsOpen} onClick={() => setProjectsOpen((v) => !v)}>
            <span className="tree-caret">{projectsOpen ? "▾" : "▸"}</span>
            {t("sidebar.projects")}
          </button>
          {projectsOpen && <ProjectsNode connectionId={c.id} />}
        </li>
        {info.data?.capabilities.listNodes && (
          <li>
            <button type="button" className="link" onClick={() => open({ kind: "nodes", connectionId: c.id })}>
              {t("sidebar.nodes")}
            </button>
          </li>
        )}
        <li>
          <button type="button" className="link" onClick={() => open({ kind: "findings", connectionId: c.id })}>
            {t("sidebar.findings")}
          </button>
        </li>
      </ul>
    </li>
  );
}

/** Проекты подключения (спец. §6.1): избранные сверху, фильтр; без прав на список — проекты из профиля. */
function ProjectsNode({ connectionId }: { connectionId: string }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [filter, setFilter] = useState("");
  const q = useQuery({ queryKey: ["projects", connectionId], queryFn: () => api.listProjects(connectionId) });
  const all = q.data?.items;
  const items = useMemo(() => {
    const f = filter.trim().toLowerCase();
    return (all ?? [])
      .filter((p) => !f || p.name.toLowerCase().includes(f) || p.displayName?.toLowerCase().includes(f))
      .sort((a, b) => Number(b.favorite) - Number(a.favorite) || a.name.localeCompare(b.name));
  }, [all, filter]);
  return (
    <>
      {q.isPending && <p className="tree-notice">{t("common.loading")}</p>}
      {q.error && (
        <>
          <p className="tree-error" role="alert">
            {tr(toAppError(q.error))}
          </p>
          <p className="tree-notice">
            <button type="button" className="link" onClick={() => void q.refetch()}>
              {t("common.retry")}
            </button>
          </p>
        </>
      )}
      {/* Кластер не отдал список: проекты берутся из профиля. Если и там пусто — одно сообщение вместо «из профиля» и «Проектов нет». */}
      {q.data?.error && <p className="tree-notice">{t(all?.length === 0 ? "sidebar.noProjectsFromProfile" : "sidebar.projectsFromProfile")}</p>}
      {all && all.length >= FILTER_FROM && (
        <input className="tree-filter" aria-label={t("sidebar.projectsFilter")} placeholder={t("sidebar.projectsFilter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
      )}
      <TruncatedNote truncated={q.data?.truncated} className="tree-notice" />
      {all && all.length === 0 && !q.data?.error && <p className="tree-notice">{t("sidebar.noProjects")}</p>}
      {all && all.length > 0 && items.length === 0 && <p className="tree-notice">{t("sidebar.noMatches")}</p>}
      <ul>
        {items.map((p) => (
          <li key={p.name}>
            <button type="button" className="link" title={p.displayName ?? undefined} onClick={() => open({ kind: "project", connectionId, namespace: p.name })}>
              {p.favorite ? `★ ${p.name}` : p.name}
            </button>
          </li>
        ))}
      </ul>
    </>
  );
}
