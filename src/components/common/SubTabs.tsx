import { Icon, type IconName } from "./Icon";

/** Подвкладка экрана объекта; `icon` — значок типа объектов (спец. U1 перенос §1). */
export interface SubTab<T extends string> {
  id: T;
  label: string;
  icon?: IconName;
}

/** Подвкладки экрана объекта: показывается одна, остальные не смонтированы (их watch не идёт). */
export function SubTabs<T extends string>({ tabs, active, onChange }: { tabs: SubTab<T>[]; active: T; onChange: (id: T) => void }) {
  return (
    <div className="subtabs" role="tablist">
      {tabs.map((tab) => (
        <button key={tab.id} type="button" role="tab" aria-selected={tab.id === active} className={tab.id === active ? "active" : undefined} onClick={() => onChange(tab.id)}>
          {tab.icon && <Icon name={tab.icon} size={14} />}
          {tab.label}
        </button>
      ))}
    </div>
  );
}
