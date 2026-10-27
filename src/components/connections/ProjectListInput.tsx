import { useId, useState } from "react";
import { parseNamespaces } from "./profile";

/** Сколько подсказок показывать: дальше сужает ввод. */
const MAX_SHOWN = 8;

/** Уже введённая часть списка и проект, который вводится сейчас (после последней запятой или пробела). */
function split(text: string): { head: string; typed: string } {
  const m = /^([\s\S]*[\s,])?([^\s,]*)$/.exec(text);
  return { head: m?.[1] ?? "", typed: m?.[2] ?? "" };
}

/**
 * Поле со списком проектов через запятую и подсказками из известных проектов: дополняется проект,
 * который вводится сейчас; уже перечисленные не предлагаются. Стрелки выбирают, Enter подставляет,
 * Escape закрывает список.
 */
export function ProjectListInput({
  id,
  value,
  onChange,
  known,
  placeholder,
}: {
  id: string;
  value: string;
  onChange: (value: string) => void;
  known: string[];
  placeholder?: string;
}) {
  const listId = useId();
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const { head, typed } = split(value);
  const listed = new Set(parseNamespaces(head));
  const needle = typed.toLowerCase();
  const options = known.filter((p) => !listed.has(p) && p !== typed && p.toLowerCase().includes(needle)).slice(0, MAX_SHOWN);
  const shown = open && options.length > 0;
  const current = Math.min(active, options.length - 1);
  const choose = (p: string) => {
    onChange(`${head}${p}, `);
    setActive(0);
  };
  return (
    <div className="combo">
      <input
        id={id}
        role="combobox"
        aria-autocomplete="list"
        aria-expanded={shown}
        aria-controls={listId}
        aria-activedescendant={shown ? `${listId}-${current}` : undefined}
        autoComplete="off"
        placeholder={placeholder}
        value={value}
        onChange={(e) => {
          onChange(e.target.value);
          setOpen(true);
          setActive(0);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => setOpen(false)}
        onKeyDown={(e) => {
          if (e.key === "ArrowDown" && !shown) {
            setOpen(true);
            e.preventDefault();
          } else if (!shown) {
            return;
          } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
            const step = e.key === "ArrowDown" ? 1 : options.length - 1;
            setActive((current + step) % options.length);
            e.preventDefault();
          } else if (e.key === "Enter") {
            choose(options[current]);
            e.preventDefault();
          } else if (e.key === "Escape") {
            setOpen(false);
            e.preventDefault();
            e.stopPropagation();
          }
        }}
      />
      {shown && (
        <ul id={listId} role="listbox" className="combo-list">
          {options.map((p, i) => (
            <li
              key={p}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={i === current}
              // mousedown, а не click: иначе поле теряет фокус и список закрывается раньше щелчка.
              onMouseDown={(e) => {
                e.preventDefault();
                choose(p);
              }}
            >
              {p}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
