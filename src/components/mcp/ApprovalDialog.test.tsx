import { act, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { MCP_STATUS_KEY } from "../../lib/useMcpStatus";
import { mcpStatus } from "../../test/mcp";
import { renderWithClient } from "../../test/render";

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return { ...orig, api: { getMcpStatus: vi.fn(), answerMcpApproval: vi.fn() } };
});

import { api } from "../../api/client";
import { ApprovalDialog } from "./ApprovalDialog";

beforeEach(() => vi.clearAllMocks());

describe("ApprovalDialog", () => {
  it("спрашивает о новом клиенте и отправляет ответ", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({
        pending: [
          { sessionId: "s1", clientName: "claude-ai", clientVersion: "1.2", },
          { sessionId: "s2", clientName: "cursor", clientVersion: "3" },
        ],
      }),
    );
    vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<ApprovalDialog />);
    const dialog = await screen.findByRole("dialog", { name: "claude-ai 1.2 хочет подключиться к Glafira" });
    expect(dialog).toHaveTextContent("открытых для LLM-ассистентов: 2");
    expect(dialog).toHaveTextContent("Ждут ответа ещё: 1");
    await user.click(screen.getByRole("button", { name: "Разрешить" }));
    expect(api.answerMcpApproval).toHaveBeenCalledWith("s1", true);
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  });

  it("клиент без имени: ответ только на сеанс", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(
      mcpStatus({ pending: [{ sessionId: "s9", clientName: null, clientVersion: null }] }),
    );
    vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
    const user = userEvent.setup();
    renderWithClient(<ApprovalDialog />);
    expect(await screen.findByRole("dialog", { name: "Неизвестный MCP-клиент хочет подключиться к Glafira" })).toHaveTextContent(
      "только на этот сеанс",
    );
    await user.click(screen.getByRole("button", { name: "Отклонить" }));
    expect(api.answerMcpApproval).toHaveBeenCalledWith("s9", false);
  });

  it("без ожидающих запросов ничего не показывает", async () => {
    vi.mocked(api.getMcpStatus).mockResolvedValue(mcpStatus());
    const { client } = renderWithClient(<ApprovalDialog />);
    await waitFor(() => expect(client.getQueryData(MCP_STATUS_KEY)).toBeDefined());
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  describe("клавиатура и фокус", () => {
    const pendingOne = () =>
      vi.mocked(api.getMcpStatus).mockResolvedValue(
        mcpStatus({ pending: [{ sessionId: "s1", clientName: "claude-ai", clientVersion: "1.2" }] }),
      );

    it("при появлении фокус на самом диалоге, а не на «Разрешить»", async () => {
      pendingOne();
      renderWithClient(<ApprovalDialog />);
      const dialog = await screen.findByRole("dialog");
      expect(dialog).toHaveFocus();
      expect(screen.getByRole("button", { name: "Разрешить" })).not.toHaveFocus();
    });

    it("Escape отклоняет текущий запрос и никогда не разрешает", async () => {
      pendingOne();
      vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
      const user = userEvent.setup();
      renderWithClient(<ApprovalDialog />);
      await screen.findByRole("dialog");
      await user.keyboard("{Escape}");
      expect(api.answerMcpApproval).toHaveBeenCalledTimes(1);
      expect(api.answerMcpApproval).toHaveBeenCalledWith("s1", false);
    });

    it("Escape не отправляет второй ответ, пока первый в пути", async () => {
      pendingOne();
      vi.mocked(api.answerMcpApproval).mockReturnValue(new Promise(() => {}));
      const user = userEvent.setup();
      renderWithClient(<ApprovalDialog />);
      await user.click(await screen.findByRole("button", { name: "Отклонить" }));
      await waitFor(() => expect(screen.getByRole("button", { name: "Отклонить" })).toBeDisabled());
      await user.keyboard("{Escape}");
      expect(api.answerMcpApproval).toHaveBeenCalledTimes(1);
    });

    it("Tab с последней кнопки возвращается к первой внутри диалога", async () => {
      pendingOne();
      const user = userEvent.setup();
      renderWithClient(
        <>
          <button type="button">Позади диалога</button>
          <ApprovalDialog />
        </>,
      );
      await screen.findByRole("dialog");
      await user.tab();
      expect(screen.getByRole("button", { name: "Разрешить" })).toHaveFocus();
      await user.tab();
      expect(screen.getByRole("button", { name: "Отклонить" })).toHaveFocus();
      await user.tab();
      expect(screen.getByRole("button", { name: "Разрешить" })).toHaveFocus();
    });

    it("Shift+Tab с самого диалога переходит на последнюю кнопку, а не на страницу позади", async () => {
      pendingOne();
      const user = userEvent.setup();
      renderWithClient(
        <>
          <button type="button">Позади диалога</button>
          <ApprovalDialog />
        </>,
      );
      await screen.findByRole("dialog");
      await user.tab({ shift: true });
      expect(screen.getByRole("button", { name: "Отклонить" })).toHaveFocus();
    });

    it("после ответа фокус возвращается туда, где был до диалога", async () => {
      pendingOne();
      vi.mocked(api.answerMcpApproval).mockResolvedValue(mcpStatus());
      const user = userEvent.setup();
      renderWithClient(
        <>
          <button type="button">Страница</button>
          <ApprovalDialog />
        </>,
      );
      const page = screen.getByRole("button", { name: "Страница" });
      page.focus();
      await screen.findByRole("dialog");
      expect(page).not.toHaveFocus();
      await user.click(screen.getByRole("button", { name: "Разрешить" }));
      await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
      expect(page).toHaveFocus();
    });

    it("со следующим запросом фокус снова на диалоге, а ошибка прошлого ответа сброшена", async () => {
      vi.mocked(api.getMcpStatus).mockResolvedValue(
        mcpStatus({
          pending: [
            { sessionId: "s1", clientName: "claude-ai", clientVersion: "1.2" },
            { sessionId: "s2", clientName: "cursor", clientVersion: "3" },
          ],
        }),
      );
      vi.mocked(api.answerMcpApproval).mockRejectedValue({ code: "MCP_FAILED", message: "boom" });
      const user = userEvent.setup();
      const { client } = renderWithClient(<ApprovalDialog />);
      await user.click(await screen.findByRole("button", { name: "Разрешить" }));
      expect(await screen.findByRole("alert")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Разрешить" })).toHaveFocus();

      act(() => {
        client.setQueryData(MCP_STATUS_KEY, mcpStatus({ pending: [{ sessionId: "s2", clientName: "cursor", clientVersion: "3" }] }));
      });
      const dialog = await screen.findByRole("dialog", { name: "cursor 3 хочет подключиться к Glafira" });
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
      expect(dialog).toHaveFocus();
    });
  });
});
