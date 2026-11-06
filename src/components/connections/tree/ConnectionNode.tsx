import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { type HTMLAttributes, type ReactNode, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { api, toAppError } from "../../../api/client";
import type { ConnectionProfile } from "../../../api/types";
import { tr } from "../../../i18n/text";
import { needsSignIn } from "../../../lib/signIn";
import { type TabSpec, useWorkspace } from "../../../state/workspace";
import { reconnect, sessionMutationKey } from "../../cluster/SessionActions";
import { EnvBadge } from "../../common/EnvBadge";
import { Icon, type IconName } from "../../common/Icon";
import { TruncatedNote } from "../../common/TruncatedNote";
import { matchProject } from "../../health/findings";
import { HealthBadge } from "../../health/HealthBlock";
import { TreeRow } from "./TreeRow";

/** С какого числа проектов показывается фильтр. */
const FILTER_FROM = 8;

/**
 * Узел дерева, которому принадлежит активная вкладка (спец. U1 §6): `cluster`, `nodes`, `findings` или
 * `project:<namespace>` — у вкладок workload'а и пода своих узлов нет, подсвечивается их проект. Строка,
 * а не объект: селектор zustand должен возвращать одно и то же значение, пока состояние не изменилось.
 */
function useActiveNode(connectionId: string): string | null {
  return useWorkspace((s) => {
    const tab = s.tabs.find((x) => x.id === s.activeId);
    if (!tab || !("connectionId" in tab) || tab.connectionId !== connectionId) return null;
    switch (tab.kind) {
      case "cluster":
      case "nodes":
      case "findings":
        return tab.kind;
      case "project":
      case "workload":
      case "pod":
        return `project:${tab.namespace}`;
      default:
        return null;
    }
  });
}

/**
 * Узел подключения в дереве: точка цвета, имя, окружение и пункты подключения — «Обзор», «Проекты»
 * (разворачиваются и читаются по щелчку) и «Ноды», если на них есть права. rowProps — атрибуты строки
 * (перетаскивание). Пункт активной вкладки подсвечен (`aria-current`).
 */
export function ConnectionNode({ connection: c, rowProps }: { connection: ConnectionProfile; rowProps?: HTMLAttributes<HTMLDivElement> }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const [projectsOpen, setProjectsOpen] = useState(false);
  const active = useActiveNode(c.id);
  const activeProject = active?.startsWith("project:") ? active.slice("project:".length) : null;
  // Сведения о подключении — из вкладки подключения; само дерево не подключается.
  const info = useQuery({ queryKey: ["connection", c.id], queryFn: () => api.connect(c.id, false), enabled: false });

  const leaf = (kind: "cluster" | "nodes" | "findings", icon: IconName, name: string, spec: TabSpec, extra?: ReactNode) => (
    <li>
      <TreeRow icon={icon} name={name} extra={extra} current={active === kind} onActivate={() => open(spec)} />
    </li>
  );

  return (
    <li>
      <TreeRow
        chevron="leaf"
        icon={<span className="dot" style={{ background: c.color }} />}
        name={c.name}
        title={c.name}
        rowProps={{ ...rowProps, className: rowProps?.className ? `tree-conn ${rowProps.className}` : "tree-conn" }}
      >
        <EnvBadge environment={c.environment} readOnly={c.readOnly} />
        {(c.llmAccess === "metadata" || c.llmAccess === "logs") && (
          <span
            className="llm-badge"
            role="img"
            title={t(c.llmAccess === "logs" ? "sidebar.llmAccessLogs" : "sidebar.llmAccess")}
            aria-label={t(c.llmAccess === "logs" ? "sidebar.llmAccessLogs" : "sidebar.llmAccess")}
          >
            LLM
          </span>
        )}
        <button
          type="button"
          className="icon"
          title={t("sidebar.editConnection")}
          aria-label={t("sidebar.editConnection")}
          onClick={() => open({ kind: "connection-form", connectionId: c.id })}
        >
          <Icon name="pencil" size={14} />
        </button>
      </TreeRow>
      <ul>
        {leaf("cluster", "grid", t("sidebar.cluster"), { kind: "cluster", connectionId: c.id })}
        <li>
          {/* Пока проекты свёрнуты, вкладка проекта подсвечивает их строку — иначе у неё не было бы следа в дереве. */}
          <TreeRow
            chevron={{ expanded: projectsOpen, onToggle: () => setProjectsOpen((v) => !v), label: t("sidebar.folderToggle", { name: t("sidebar.projects") }) }}
            icon="namespace"
            name={t("sidebar.projects")}
            current={!projectsOpen && activeProject !== null}
          />
          {projectsOpen && <ProjectsNode connectionId={c.id} activeProject={activeProject} />}
        </li>
        {info.data?.capabilities.listNodes && leaf("nodes", "server", t("sidebar.nodes"), { kind: "nodes", connectionId: c.id })}
        {leaf("findings", "flag", t("sidebar.findings"), { kind: "findings", connectionId: c.id }, <HealthBadge connectionId={c.id} />)}
      </ul>
    </li>
  );
}

/**
 * Проекты подключения (спец. §6.1): избранные сверху (значок `star`), фильтр; без прав на список — проекты из
 * профиля. Сессия истекла — «Войти снова», как у `ReadError`: подключается заново и открывает вкладку подключения,
 * где видны способы войти; прочие ошибки — «Повторить».
 */
function ProjectsNode({ connectionId, activeProject }: { connectionId: string; activeProject: string | null }) {
  const { t } = useTranslation();
  const open = useWorkspace((s) => s.open);
  const queryClient = useQueryClient();
  const [filter, setFilter] = useState("");
  const q = useQuery({ queryKey: ["projects", connectionId], queryFn: () => api.listProjects(connectionId) });
  // Тот же ключ, что у входа на вкладке подключения: пока идёт подключение, второе рядом не запускается.
  const signIn = useMutation({ mutationKey: sessionMutationKey(connectionId), mutationFn: () => reconnect(queryClient, connectionId) });
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
            {needsSignIn(toAppError(q.error)) ? (
              <button
                type="button"
                className="link"
                disabled={signIn.isPending}
                onClick={() => {
                  signIn.mutate();
                  open({ kind: "cluster", connectionId });
                }}
              >
                {t("objects.signInAgain")}
              </button>
            ) : (
              <button type="button" className="link" onClick={() => void q.refetch()}>
                {t("common.retry")}
              </button>
            )}
          </p>
        </>
      )}
      {/* Кластер не отдал список: проекты берутся из профиля. Если и там пусто — одно сообщение вместо «из профиля» и «Проектов нет». */}
      {q.data?.error && <p className="tree-notice">{t(all?.length === 0 ? "sidebar.noProjectsFromProfile" : "sidebar.projectsFromProfile")}</p>}
      {all && all.length >= FILTER_FROM && (
        <div className="search-field tree-filter-wrap">
          <Icon name="search" />
          <input type="search" aria-label={t("sidebar.projectsFilter")} placeholder={t("sidebar.projectsFilter")} value={filter} onChange={(e) => setFilter(e.target.value)} />
        </div>
      )}
      <TruncatedNote truncated={q.data?.truncated} className="tree-notice" />
      {all && all.length === 0 && !q.data?.error && <p className="tree-notice">{t("sidebar.noProjects")}</p>}
      {all && all.length > 0 && items.length === 0 && <p className="tree-notice">{t("sidebar.noMatches")}</p>}
      <ul className={activeProject !== null ? "tree-open" : undefined}>
        {items.map((p) => (
          <li key={p.name}>
            <TreeRow
              icon={p.favorite ? "star" : "namespace"}
              name={p.name}
              title={p.displayName ?? p.name}
              extra={<HealthBadge connectionId={connectionId} match={matchProject(p.name)} />}
              current={activeProject === p.name}
              onActivate={() => open({ kind: "project", connectionId, namespace: p.name })}
            />
          </li>
        ))}
      </ul>
    </>
  );
}
