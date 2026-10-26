import { useVirtualizer } from "@tanstack/react-virtual";
import { type KeyboardEvent, type ReactNode, useRef } from "react";
import { useTranslation } from "react-i18next";

const ROW_HEIGHT = 28;

export interface Column<T> {
  key: string;
  title: ReactNode;
  /** Ширина в `grid-template-columns`: `120px`, `minmax(160px, 1fr)`. */
  width: string;
  render: (row: T) => ReactNode;
}

/** Таблица большого списка (спец. §6.7): строки виртуализированы, шапка закреплена, строку можно выбрать мышью и с клавиатуры. */
export function VirtualTable<T>({
  rows,
  columns,
  rowKey,
  onRowClick,
  selectedKey,
  label,
  empty,
}: {
  rows: T[];
  columns: Column<T>[];
  rowKey: (row: T) => string;
  onRowClick?: (row: T) => void;
  selectedKey?: string | null;
  label: string;
  empty?: string;
}) {
  const { t } = useTranslation();
  const parentRef = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 20,
  });
  const grid = { gridTemplateColumns: columns.map((c) => c.width).join(" ") };
  const activate = (row: T) => ({
    tabIndex: 0,
    onClick: () => onRowClick?.(row),
    onKeyDown: (e: KeyboardEvent) => {
      // Enter и пробел от кнопки или ссылки внутри ячейки её же и нажимают: строку они не выбирают.
      if (e.target !== e.currentTarget) return;
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onRowClick?.(row);
      }
    },
  });
  return (
    <div className="vlist" ref={parentRef} role="table" aria-label={label}>
      <div className="row header" role="row" style={{ ...grid, height: ROW_HEIGHT }}>
        {columns.map((c) => (
          <span key={c.key} role="columnheader">
            {c.title}
          </span>
        ))}
      </div>
      {rows.length === 0 && <p className="empty">{empty ?? t("common.empty")}</p>}
      <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
        {virtualizer.getVirtualItems().map((item) => {
          const row = rows[item.index];
          const key = rowKey(row);
          const selected = key === selectedKey;
          return (
            <div
              key={key}
              role="row"
              className={`row${onRowClick ? " clickable" : ""}${selected ? " selected" : ""}`}
              style={{ ...grid, top: item.start, height: ROW_HEIGHT }}
              aria-selected={onRowClick ? selected : undefined}
              {...(onRowClick ? activate(row) : {})}
            >
              {columns.map((c) => (
                <span key={c.key} role="cell">
                  {c.render(row)}
                </span>
              ))}
            </div>
          );
        })}
      </div>
    </div>
  );
}
