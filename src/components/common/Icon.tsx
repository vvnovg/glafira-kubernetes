import type { ReactNode, SVGAttributes } from "react";

/**
 * Набор значков интерфейса (спец. U1 §5): SVG 16×16, обводка 1.5, цвет `currentColor`. Значок без подписи
 * всегда лежит внутри кнопки с `aria-label`; сам он скрыт от экранного диктора.
 */
export type IconName =
  | "plus"
  | "folderPlus"
  | "folder"
  | "more"
  | "pencil"
  | "close"
  | "chevron"
  | "warning"
  | "lock"
  | "help"
  | "refresh"
  | "send"
  | "users"
  | "play"
  | "stop"
  | "copy"
  | "search"
  | "settings"
  | "heart"
  | "book"
  | "info"
  | "grid"
  | "list"
  | "flag"
  | "dot"
  // Значки Kubernetes-версии (спец. переноса U1 §3): узлы дерева, подвкладки, состояния.
  | "server"
  | "namespace"
  | "star"
  | "cube"
  | "pod"
  | "logs"
  | "clock"
  | "network"
  | "config"
  | "storage"
  | "quota"
  | "chart"
  | "code"
  | "check"
  | "error"
  | "hourglass";

const PATHS: Record<IconName, ReactNode> = {
  plus: <path d="M8 3v10M3 8h10" />,
  folderPlus: (
    <>
      <path d="M2 4.5h4l1.5 1.5H14v7.5H2z" />
      <path d="M8 8v4M6 10h4" />
    </>
  ),
  folder: <path d="M2 4.5h4l1.5 1.5H14v7.5H2z" />,
  more: (
    <>
      <circle cx="3.5" cy="8" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="8" cy="8" r="1.4" fill="currentColor" stroke="none" />
      <circle cx="12.5" cy="8" r="1.4" fill="currentColor" stroke="none" />
    </>
  ),
  pencil: <path d="M11 2.5l2.5 2.5L5.5 13H3v-2.5z" />,
  close: <path d="M4 4l8 8M12 4l-8 8" />,
  chevron: <path d="M6 4l4 4-4 4" />,
  warning: (
    <>
      <path d="M8 2.5l6 11H2z" />
      <path d="M8 6.5v3.2" />
      <circle cx="8" cy="11.6" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  lock: (
    <>
      <rect x="3" y="7" width="10" height="7" rx="1.5" />
      <path d="M5.5 7V5a2.5 2.5 0 0 1 5 0v2" />
    </>
  ),
  help: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M6.3 6.3a1.7 1.7 0 1 1 2.6 1.4c-.6.4-.9.8-.9 1.3" />
      <circle cx="8" cy="11.3" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  refresh: (
    <>
      <path d="M13 8a5 5 0 1 1-1.6-3.7" />
      <path d="M13 2.5v3h-3" />
    </>
  ),
  send: <path d="M2.5 8L14 2.5 10 13.5 8 9z" />,
  users: (
    <>
      <circle cx="6" cy="5.5" r="2.3" />
      <path d="M1.8 13c.4-2.3 2.1-3.5 4.2-3.5s3.8 1.2 4.2 3.5" />
      <circle cx="11.5" cy="5.5" r="1.8" />
      <path d="M11 9.6c1.9 0 3 1.1 3.3 3" />
    </>
  ),
  play: <path d="M4 2.5v11l9-5.5z" fill="currentColor" stroke="none" />,
  stop: <rect x="3" y="3" width="10" height="10" rx="1.5" fill="currentColor" stroke="none" />,
  copy: (
    <>
      <rect x="5" y="5" width="8" height="8" rx="1.5" />
      <path d="M3 11V4a1 1 0 0 1 1-1h7" />
    </>
  ),
  search: (
    <>
      <circle cx="7" cy="7" r="4.5" />
      <path d="M10.5 10.5L14 14" />
    </>
  ),
  settings: (
    <>
      <path d="M2 4.5h12M2 8h12M2 11.5h12" />
      <circle cx="6" cy="4.5" r="1.5" fill="var(--sidebar, var(--bg))" />
      <circle cx="11" cy="8" r="1.5" fill="var(--sidebar, var(--bg))" />
      <circle cx="5" cy="11.5" r="1.5" fill="var(--sidebar, var(--bg))" />
    </>
  ),
  heart: <path d="M8 13.5S2.5 10 2.5 6.2A2.9 2.9 0 0 1 8 4.6a2.9 2.9 0 0 1 5.5 1.6C13.5 10 8 13.5 8 13.5z" />,
  book: <path d="M2.5 3h4a1.5 1.5 0 0 1 1.5 1.5V13a1.5 1.5 0 0 0-1.5-1.5h-4zM13.5 3h-4A1.5 1.5 0 0 0 8 4.5V13a1.5 1.5 0 0 1 1.5-1.5h4z" />,
  info: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M8 7v4" />
      <circle cx="8" cy="5" r=".6" fill="currentColor" stroke="none" />
    </>
  ),
  grid: (
    <>
      <rect x="2.5" y="2.5" width="5" height="5" rx="1" />
      <rect x="8.5" y="2.5" width="5" height="5" rx="1" />
      <rect x="2.5" y="8.5" width="5" height="5" rx="1" />
      <rect x="8.5" y="8.5" width="5" height="5" rx="1" />
    </>
  ),
  list: <path d="M2.5 4h11M2.5 8h11M2.5 12h7" />,
  flag: (
    <>
      <path d="M4 2.5v11" />
      <path d="M4 3h8l-2 3 2 3H4" />
    </>
  ),
  dot: <circle cx="8" cy="8" r="4" fill="currentColor" stroke="none" />,
  server: (
    <>
      <rect x="2.5" y="2.5" width="11" height="4.5" rx="1" />
      <rect x="2.5" y="9" width="11" height="4.5" rx="1" />
      <circle cx="5" cy="4.75" r=".7" fill="currentColor" stroke="none" />
      <circle cx="5" cy="11.25" r=".7" fill="currentColor" stroke="none" />
    </>
  ),
  namespace: <rect x="2.5" y="2.5" width="11" height="11" rx="2" strokeDasharray="2.6 2" />,
  star: <path d="M8 2.2l1.8 3.7 4 .6-2.9 2.8.7 4L8 11.4l-3.6 1.9.7-4L2.2 6.5l4-.6z" fill="currentColor" stroke="none" />,
  cube: (
    <>
      <path d="M8 2.2l5.2 3v5.6l-5.2 3-5.2-3V5.2z" />
      <path d="M2.8 5.2L8 8.2l5.2-3M8 8.2v5.6" />
    </>
  ),
  pod: (
    <>
      <path d="M8 2.2l5 2.9v5.8l-5 2.9-5-2.9V5.1z" />
      <circle cx="8" cy="8" r="1.4" fill="currentColor" stroke="none" />
    </>
  ),
  logs: (
    <>
      <path d="M2.5 4.5l3 2.5-3 2.5" />
      <path d="M7.5 9.5h6M2.5 13h11" />
    </>
  ),
  clock: (
    <>
      <circle cx="8" cy="8" r="5.5" />
      <path d="M8 5v3.2l2.2 1.3" />
    </>
  ),
  network: (
    <>
      <circle cx="8" cy="3.5" r="1.6" />
      <circle cx="3.5" cy="12.5" r="1.6" />
      <circle cx="12.5" cy="12.5" r="1.6" />
      <path d="M7.2 4.9L4.3 11M8.8 4.9l2.9 6.1M5.1 12.5h5.8" />
    </>
  ),
  config: (
    <>
      <path d="M2.5 5h11M2.5 11h11" />
      <circle cx="6" cy="5" r="1.6" fill="var(--bg)" />
      <circle cx="10.5" cy="11" r="1.6" fill="var(--bg)" />
    </>
  ),
  storage: (
    <>
      <ellipse cx="8" cy="4" rx="5.5" ry="2" />
      <path d="M2.5 4v8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2V4" />
      <path d="M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2" />
    </>
  ),
  quota: (
    <>
      <path d="M2.5 11a5.5 5.5 0 1 1 11 0" />
      <path d="M8 11l2.5-3.5" />
      <circle cx="8" cy="11" r=".8" fill="currentColor" stroke="none" />
    </>
  ),
  chart: (
    <>
      <path d="M2.5 13.5h11" />
      <rect x="3.5" y="8" width="2.5" height="4" rx=".5" />
      <rect x="7" y="4.5" width="2.5" height="7.5" rx=".5" />
      <rect x="10.5" y="6.5" width="2.5" height="5.5" rx=".5" />
    </>
  ),
  code: <path d="M5.5 4.5L2 8l3.5 3.5M10.5 4.5L14 8l-3.5 3.5" />,
  check: <path d="M3 8.5l3.2 3L13 4.5" />,
  error: (
    <>
      <circle cx="8" cy="8" r="6" />
      <path d="M5.8 5.8l4.4 4.4M10.2 5.8l-4.4 4.4" />
    </>
  ),
  hourglass: <path d="M4 2.5h8M4 13.5h8M5 2.5v2.3c0 1.2.7 2 1.8 2.6L8 8l1.2-.6C10.3 6.8 11 6 11 4.8V2.5M5 13.5v-2.3c0-1.2.7-2 1.8-2.6L8 8l1.2.6c1.1.6 1.8 1.4 1.8 2.6v2.3" />,
};

export const ICON_NAMES = Object.keys(PATHS) as IconName[];

interface Props extends Omit<SVGAttributes<SVGSVGElement>, "children"> {
  name: IconName;
  size?: number;
}

export function Icon({ name, size = 16, className, ...rest }: Props) {
  return (
    <svg
      className={className ? `icon-svg ${className}` : "icon-svg"}
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.5}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
      data-icon={name}
      {...rest}
    >
      {PATHS[name]}
    </svg>
  );
}
