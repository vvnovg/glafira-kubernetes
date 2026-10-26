import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

vi.mock("@tanstack/react-virtual", () => ({
  useVirtualizer: (o: { count: number }) => ({
    getVirtualItems: () => Array.from({ length: o.count }, (_, index) => ({ index, start: index * 28 })),
    getTotalSize: () => o.count * 28,
  }),
}));

import { RefreshControls } from "./RefreshControls";
import { VirtualTable } from "./VirtualTable";

const rows = [
  { name: "web", ready: "1/1" },
  { name: "api", ready: "0/1" },
];
const columns = [
  { key: "name", title: "Имя", width: "1fr", render: (r: (typeof rows)[number]) => r.name },
  { key: "ready", title: "Готово", width: "60px", render: (r: (typeof rows)[number]) => r.ready },
];

describe("VirtualTable", () => {
  it("шапка, строки, выбор мышью и клавиатурой", async () => {
    const onRowClick = vi.fn();
    const user = userEvent.setup();
    render(<VirtualTable label="Поды" rows={rows} columns={columns} rowKey={(r) => r.name} onRowClick={onRowClick} selectedKey="api" />);
    expect(screen.getByRole("table", { name: "Поды" })).toBeInTheDocument();
    expect(screen.getAllByRole("columnheader").map((h) => h.textContent)).toEqual(["Имя", "Готово"]);
    const [header, web, api] = screen.getAllByRole("row");
    expect(header).not.toHaveAttribute("tabindex");
    expect(api).toHaveAttribute("aria-selected", "true");
    await user.click(web);
    expect(onRowClick).toHaveBeenCalledWith(rows[0]);
    api.focus();
    await user.keyboard("{Enter}");
    expect(onRowClick).toHaveBeenLastCalledWith(rows[1]);
  });

  it("пробел выбирает строку, Enter и пробел от кнопки внутри ячейки — нет", async () => {
    const onRowClick = vi.fn();
    const onInner = vi.fn();
    const user = userEvent.setup();
    const withButton = [
      columns[0],
      {
        key: "act",
        title: "Действие",
        width: "80px",
        render: (r: (typeof rows)[number]) => (
          <button type="button" onClick={(e) => (e.stopPropagation(), onInner(r.name))}>
            {`Открыть ${r.name}`}
          </button>
        ),
      },
    ];
    render(<VirtualTable label="Поды" rows={rows} columns={withButton} rowKey={(r) => r.name} onRowClick={onRowClick} />);
    const [, web] = screen.getAllByRole("row");
    web.focus();
    await user.keyboard(" ");
    expect(onRowClick).toHaveBeenCalledTimes(1);
    expect(onRowClick).toHaveBeenLastCalledWith(rows[0]);
    // Пробел на строке не прокручивает список.
    const space = new KeyboardEvent("keydown", { key: " ", bubbles: true, cancelable: true });
    web.dispatchEvent(space);
    expect(space.defaultPrevented).toBe(true);
    expect(onRowClick).toHaveBeenCalledTimes(2);

    // Кнопка внутри ячейки получает свои Enter и пробел как есть: строка не выбирается, действие по умолчанию не отменяется.
    onRowClick.mockClear();
    const button = screen.getByRole("button", { name: "Открыть api" });
    const enter = new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true });
    button.dispatchEvent(enter);
    const spaceOnButton = new KeyboardEvent("keydown", { key: " ", bubbles: true, cancelable: true });
    button.dispatchEvent(spaceOnButton);
    expect(enter.defaultPrevented).toBe(false);
    expect(spaceOnButton.defaultPrevented).toBe(false);
    button.focus();
    await user.keyboard("{Enter}");
    await user.keyboard(" ");
    expect(onInner).toHaveBeenCalledTimes(2);
    expect(onInner).toHaveBeenCalledWith("api");
    expect(onRowClick).not.toHaveBeenCalled();
  });

  it("пустой список и автообновление", async () => {
    const onInterval = vi.fn();
    const onRefresh = vi.fn();
    const user = userEvent.setup();
    render(
      <>
        <VirtualTable label="Поды" rows={[]} columns={columns} rowKey={(r) => r.name} />
        <RefreshControls interval={0} onInterval={onInterval} onRefresh={onRefresh} busy={false} />
      </>,
    );
    expect(screen.getByText("Пусто")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("Автообновление"), "каждые 10 с");
    expect(onInterval).toHaveBeenCalledWith(10_000);
    await user.click(screen.getByRole("button", { name: "Обновить" }));
    expect(onRefresh).toHaveBeenCalled();
  });
});
