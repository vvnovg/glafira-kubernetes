import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ConnectionFolder, ImportPreview } from "../../../api/types";
import { useConnectionEpoch } from "../../../state/connectionEpoch";
import { renderWithClient } from "../../../test/render";

vi.mock("../../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../../api/client")>();
  return {
    ...orig,
    api: {
      connect: vi.fn(),
      listFolders: vi.fn(),
      previewImport: vi.fn(),
      applyImport: vi.fn(),
    },
  };
});

import { api } from "../../../api/client";
import { ImportDialog } from "./ImportDialog";

const FOLDERS: ConnectionFolder[] = [
  { id: "a", name: "Проект А", parentId: null, order: 0 },
  { id: "b", name: "ИФТ", parentId: "a", order: 0 },
];

const PREVIEW: ImportPreview = {
  added: [
    { kind: "folder", id: "f1", name: "Новая", path: [] },
    { kind: "connection", id: "c1", name: "ocp-new", path: ["Новая"] },
  ],
  updated: [{ kind: "connection", id: "c2", name: "ocp-upd", path: ["Проект А"] }],
  unchanged: [],
  skipped: [
    {
      name: "broken",
      reason: { key: "errors.import.unsupportedConnection", params: { error: "oops" }, text: "Unsupported: oops" },
    },
  ],
  warnings: [{ key: "errors.generic", params: { message: "Предупреждение из файла" }, text: "Warning from the file" }],
  needsSecrets: 3,
};

const PATH = "/tmp/in.glafira-kubernetes-connections.json";

/** Открытая вкладка подключения «c2» (оно обновляется импортом): её данные подписаны на `["connection", id]`. */
function OpenTab() {
  const q = useQuery({ queryKey: ["connection", "c2"], queryFn: () => api.connect("c2", false) });
  return <output aria-label="Вкладка">{q.data === undefined ? "…" : String(q.data)}</output>;
}

beforeEach(() => {
  vi.clearAllMocks();
  useConnectionEpoch.setState({ epochs: {} });
  vi.mocked(api.listFolders).mockResolvedValue(FOLDERS);
  vi.mocked(api.previewImport).mockResolvedValue(PREVIEW);
  vi.mocked(api.applyImport).mockResolvedValue(PREVIEW);
});

function renderDialog(targetId: string | null = null) {
  const onClose = vi.fn();
  const onImported = vi.fn();
  const r = renderWithClient(<ImportDialog path={PATH} initialTargetId={targetId} onClose={onClose} onImported={onImported} />);
  return { ...r, onClose, onImported };
}

describe("ImportDialog", () => {
  it("показывает сводку и группы из предпросмотра", async () => {
    renderDialog();
    const dialog = screen.getByRole("dialog", { name: "Импорт подключений" });
    expect(await within(dialog).findByText("Добавится: 2 · Обновится: 1 · Без изменений: 0 · Пропущено: 1")).toBeInTheDocument();
    expect(api.previewImport).toHaveBeenCalledWith(PATH, null, true);
    expect(within(dialog).getByText("Новые (2)")).toBeInTheDocument();
    expect(within(dialog).getByText("Обновятся (1)")).toBeInTheDocument();
    expect(within(dialog).getByText("Пропущены (1)")).toBeInTheDocument();
    expect(within(dialog).queryByText(/^Без изменений \(/)).not.toBeInTheDocument();
    expect(within(dialog).getByText("Новая / ocp-new")).toBeInTheDocument();
    expect(within(dialog).getByText("Проект А / ocp-upd")).toBeInTheDocument();
    expect(within(dialog).getByText(/broken/)).toHaveTextContent("oops");
  });

  it("смена цели и флажка повторяет предпросмотр с новыми аргументами", async () => {
    const user = userEvent.setup();
    renderDialog();
    await screen.findByText(/^Добавится:/);
    const select = screen.getByRole("combobox", { name: "Куда импортировать" });
    expect(within(select).getByRole("option", { name: "Проект А / ИФТ" })).toBeInTheDocument();
    await user.selectOptions(select, "b");
    await waitFor(() => expect(api.previewImport).toHaveBeenLastCalledWith(PATH, "b", true));
    await user.click(screen.getByRole("checkbox", { name: "Обновлять существующие подключения" }));
    await waitFor(() => expect(api.previewImport).toHaveBeenLastCalledWith(PATH, "b", false));
  });

  it("при «Импортировать сюда» выбрана папка", async () => {
    renderDialog("a");
    await waitFor(() => expect(api.previewImport).toHaveBeenCalledWith(PATH, "a", true));
    expect(await screen.findByRole("combobox", { name: "Куда импортировать" })).toHaveValue("a");
  });

  it("показывает предупреждения и строку о секретах", async () => {
    renderDialog();
    expect(await screen.findByText("Предупреждение из файла")).toBeInTheDocument();
    expect(screen.getByText("После импорта введите токены или ключи для 3 подключений")).toBeInTheDocument();
  });

  it("«Импортировать» применяет импорт с теми же аргументами и закрывает диалог", async () => {
    const user = userEvent.setup();
    const { onImported, client } = renderDialog();
    const invalidate = vi.spyOn(client, "invalidateQueries");
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("checkbox", { name: "Обновлять существующие подключения" }));
    await waitFor(() => expect(api.previewImport).toHaveBeenLastCalledWith(PATH, null, false));
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    expect(api.applyImport).toHaveBeenCalledWith(PATH, null, false);
    await waitFor(() => expect(onImported).toHaveBeenCalledWith(PREVIEW));
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["connections"] });
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["folders"] });
  });

  it("предупреждает о смене адреса подключения с сохранёнными учётными данными", async () => {
    vi.mocked(api.previewImport).mockResolvedValue({
      ...PREVIEW,
      warnings: [
        {
          key: "messages.import.addressChanged",
          params: { name: "prod", from: "https://api.old:6443", to: "https://api.new:6443" },
          text: "English fallback",
        },
      ],
    });
    renderDialog();
    expect(
      await screen.findByText(
        "«prod»: адрес API меняется с https://api.old:6443 на https://api.new:6443. Сохранённые учётные данные (токен, ключ или подтверждённый exec-плагин) будут отправлены на новый адрес.",
      ),
    ).toBeInTheDocument();
  });

  it("после импорта сбрасывает кэш обновлённых подключений", async () => {
    let cached: unknown[] = [];
    const onImported = vi.fn(() => {
      cached = [rendered.client.getQueryData(["connection", "c2"]), rendered.client.getQueryData(["namespaces", "c2"])];
    });
    const user = userEvent.setup();
    const rendered = renderWithClient(<ImportDialog path={PATH} initialTargetId={null} onClose={vi.fn()} onImported={onImported} />);
    rendered.client.setQueryData(["connection", "c2"], { stale: true });
    rendered.client.setQueryData(["namespaces", "c2"], ["a"]);
    // Добавленное и посторонние подключения не затрагиваются.
    rendered.client.setQueryData(["connection", "c1"], { keep: true });
    rendered.client.setQueryData(["connection", "other"], { keep: true });
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    await waitFor(() => expect(onImported).toHaveBeenCalled());
    // К моменту onImported открытая вкладка подключения уже не видит прежний адрес и вход.
    expect(cached).toEqual([undefined, undefined]);
    expect(rendered.client.getQueryData(["connection", "c1"])).toEqual({ keep: true });
    expect(rendered.client.getQueryData(["connection", "other"])).toEqual({ keep: true });
  });

  it("после импорта открытая вкладка обновлённого подключения перезапрашивает данные", async () => {
    vi.mocked(api.connect).mockResolvedValueOnce("сеанс истёк" as never).mockResolvedValue("подключено" as never);
    const user = userEvent.setup();
    renderWithClient(
      <>
        <OpenTab />
        <ImportDialog path={PATH} initialTargetId={null} onClose={vi.fn()} onImported={vi.fn()} />
      </>,
    );
    await waitFor(() => expect(screen.getByLabelText("Вкладка")).toHaveTextContent("сеанс истёк"));
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    // Подписчик ключа `["connection", "c2"]` получает свежие данные без повторного открытия вкладки.
    await waitFor(() => expect(screen.getByLabelText("Вкладка")).toHaveTextContent("подключено"));
    expect(api.connect).toHaveBeenCalledTimes(2);
  });

  it("после импорта перезапускает watch и логи только обновлённых подключений", async () => {
    // Rust остановил их сессии без события; добавленные, не изменившиеся и папки сессий не имеют.
    vi.mocked(api.applyImport).mockResolvedValue({
      ...PREVIEW,
      updated: [
        { kind: "connection", id: "c2", name: "ocp-upd", path: ["Проект А"] },
        { kind: "folder", id: "f2", name: "Старая", path: [] },
      ],
      unchanged: [{ kind: "connection", id: "c3", name: "ocp-same", path: [] }],
    });
    const user = userEvent.setup();
    const { onImported } = renderDialog();
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    await waitFor(() => expect(onImported).toHaveBeenCalled());
    expect(useConnectionEpoch.getState().epochs).toEqual({ c2: 1 });
  });

  it("сбой применения не перезапускает сессии", async () => {
    vi.mocked(api.applyImport).mockRejectedValue({ code: "INTERNAL", message: "Disk is full", retryable: false });
    const user = userEvent.setup();
    renderDialog();
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    expect(await screen.findByText("Disk is full")).toBeInTheDocument();
    expect(useConnectionEpoch.getState().epochs).toEqual({});
  });

  it("сбой применения не сбрасывает кэш подключений", async () => {
    vi.mocked(api.applyImport).mockRejectedValue({ code: "INTERNAL", message: "Disk is full", retryable: false });
    const user = userEvent.setup();
    const { client } = renderWithClient(<ImportDialog path={PATH} initialTargetId={null} onClose={vi.fn()} onImported={vi.fn()} />);
    client.setQueryData(["connection", "c2"], { keep: true });
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Импортировать" }));
    expect(await screen.findByText("Disk is full")).toBeInTheDocument();
    // Импорт записывается одной записью: после сбоя ничего не изменилось.
    expect(client.getQueryData(["connection", "c2"])).toEqual({ keep: true });
  });

  it("«Отмена» закрывает диалог без импорта", async () => {
    const user = userEvent.setup();
    const { onClose } = renderDialog();
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Отмена" }));
    expect(onClose).toHaveBeenCalled();
    expect(api.applyImport).not.toHaveBeenCalled();
  });

  it("ошибка разбора файла показывается баннером, импорт недоступен", async () => {
    vi.mocked(api.previewImport).mockRejectedValue({
      code: "INVALID_INPUT",
      key: "errors.import.incompatibleVersion",
      message: "The file was created by an incompatible Glafira version",
      retryable: false,
    });
    renderDialog();
    expect(await screen.findByRole("alert")).toHaveTextContent("Файл создан в несовместимой версии Glafira — обновите приложение");
    expect(screen.getByRole("button", { name: "Импортировать" })).toBeDisabled();
  });

  it("Tab не уводит фокус из диалога", async () => {
    const user = userEvent.setup();
    renderWithClient(
      <>
        <button type="button">снаружи</button>
        <ImportDialog path={PATH} initialTargetId={null} onClose={vi.fn()} onImported={vi.fn()} />
      </>,
    );
    await screen.findByText(/^Добавится:/);
    const firstHint = screen.getByRole("button", { name: "Подсказка: Куда импортировать" });
    const cancel = screen.getByRole("button", { name: "Отмена" });
    expect(screen.getByRole("combobox", { name: "Куда импортировать" })).toHaveFocus();
    await user.tab({ shift: true });
    expect(firstHint).toHaveFocus();
    await user.tab({ shift: true });
    expect(cancel).toHaveFocus();
    await user.tab();
    expect(firstHint).toHaveFocus();
    for (const shift of [false, true]) {
      for (let i = 0; i < 20; i++) {
        await user.tab({ shift });
        expect(screen.getByRole("dialog")).toContainElement(document.activeElement as HTMLElement);
      }
    }
  });

  it("после закрытия фокус возвращается туда, где был до открытия", async () => {
    const outside = (
      <button type="button" key="b">
        меню
      </button>
    );
    const dialog = <ImportDialog key="d" path={PATH} initialTargetId={null} onClose={vi.fn()} onImported={vi.fn()} />;
    const { rerender, client } = renderWithClient(<>{outside}</>);
    screen.getByRole("button", { name: "меню" }).focus();
    const wrap = (children: React.ReactNode) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    rerender(wrap([outside, dialog]));
    await waitFor(() => expect(screen.getByRole("combobox", { name: "Куда импортировать" })).toHaveFocus());
    rerender(wrap([outside]));
    expect(screen.getByRole("button", { name: "меню" })).toHaveFocus();
  });

  it("Esc при открытой подсказке закрывает только подсказку", async () => {
    const user = userEvent.setup();
    const { onClose } = renderDialog();
    await screen.findByText(/^Добавится:/);
    await user.click(screen.getByRole("button", { name: "Подсказка: Куда импортировать" }));
    expect(await screen.findByRole("tooltip")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    await waitFor(() => expect(screen.queryByRole("tooltip")).not.toBeInTheDocument());
    expect(onClose).not.toHaveBeenCalled();
    await user.keyboard("{Escape}");
    expect(onClose).toHaveBeenCalled();
  });
});
