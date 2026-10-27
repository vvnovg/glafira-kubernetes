import { screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { renderWithClient } from "../../test/render";

vi.mock("@tauri-apps/plugin-opener", () => ({ openUrl: vi.fn() }));

vi.mock("../../api/client", async (importOriginal) => {
  const orig = await importOriginal<typeof import("../../api/client")>();
  return {
    ...orig,
    api: {
      connect: vi.fn(),
      startOauthLogin: vi.fn(),
      cancelOauthLogin: vi.fn(),
      setConnectionToken: vi.fn(),
      oauthTokenPage: vi.fn(),
    },
  };
});

import { openUrl } from "@tauri-apps/plugin-opener";
import { api } from "../../api/client";
import type { ConnectionInfo, ConnectionProfile } from "../../api/types";
import { useConnectionEpoch } from "../../state/connectionEpoch";
import { newProfile } from "../connections/profile";
import { SessionActions } from "./SessionActions";

const INFO = { user: "dev1" } as ConnectionInfo;
const PROFILE: ConnectionProfile = { ...newProfile(), id: "c1", name: "dev-cluster", apiUrl: "https://api.dev:6443", auth: { method: "token" } };
const FAILED = { code: "CONNECTION_FAILED", key: "errors.k8s.connect", message: "x", retryable: true };
/** Ответ на обычном Kubernetes: OAuth-сервера OpenShift нет. */
const NO_OAUTH = { code: "AUTH_FAILED", key: "errors.oauth.notAvailable", message: "x", retryable: false };
const TOKEN_PAGE = "https://oauth-openshift.apps.dev/oauth/token/request";

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(api.oauthTokenPage).mockRejectedValue(NO_OAUTH);
  useConnectionEpoch.setState({ epochs: {} });
});

async function pasteToken() {
  const user = userEvent.setup();
  renderWithClient(<SessionActions profile={PROFILE} />);
  await user.type(screen.getByLabelText("Токен"), "sha256~new");
  await user.click(screen.getByRole("button", { name: "Сохранить токен" }));
}

describe("SessionActions: вставка токена и сессии списков", () => {
  it("токен сохранён и вход удался: списки перезапускаются один раз, через afterSignIn", async () => {
    vi.mocked(api.setConnectionToken).mockResolvedValue();
    vi.mocked(api.connect).mockResolvedValue(INFO);
    await pasteToken();
    await waitFor(() => expect(useConnectionEpoch.getState().epochs).toEqual({ c1: 1 }));
    expect(api.connect).toHaveBeenCalledWith("c1", true);
  });

  it("токен сохранён, но вход не удался: Rust уже остановил сессии, списки всё равно перезапускаются", async () => {
    vi.mocked(api.setConnectionToken).mockResolvedValue();
    vi.mocked(api.connect).mockRejectedValue(FAILED);
    await pasteToken();
    expect(await screen.findByText("Не удаётся подключиться к серверу")).toBeInTheDocument();
    expect(useConnectionEpoch.getState().epochs).toEqual({ c1: 1 });
  });

  it("токен не сохранён: Rust сессии не трогал, списки остаются", async () => {
    vi.mocked(api.setConnectionToken).mockRejectedValue({ code: "INTERNAL", key: "errors.secrets.keychain", message: "Keychain is locked", retryable: false });
    await pasteToken();
    expect(await screen.findByText("Ошибка доступа к системному хранилищу секретов")).toBeInTheDocument();
    expect(api.connect).not.toHaveBeenCalled();
    expect(useConnectionEpoch.getState().epochs).toEqual({});
  });
});

describe("SessionActions: страница токена — только у OAuth-сервера OpenShift", () => {
  it("OpenShift: ссылка на страницу токена открывает её в браузере, подсказка в поле — sha256~…", async () => {
    vi.mocked(api.oauthTokenPage).mockResolvedValue(TOKEN_PAGE);
    const user = userEvent.setup();
    renderWithClient(<SessionActions profile={PROFILE} />);
    await user.click(await screen.findByRole("button", { name: "Получить токен на странице кластера" }));
    expect(openUrl).toHaveBeenCalledWith(TOKEN_PAGE);
    expect(screen.getByLabelText("Токен")).toHaveAttribute("placeholder", "sha256~…");
  });

  it("Kubernetes без OAuth-сервера OpenShift: ни ссылки на страницу токена, ни подсказки sha256~…, ни ошибки", async () => {
    const { client } = renderWithClient(<SessionActions profile={PROFILE} />);
    await waitFor(() => expect(api.oauthTokenPage).toHaveBeenCalledWith("c1"));
    await waitFor(() => expect(client.isFetching()).toBe(0));
    expect(screen.queryByRole("button", { name: "Получить токен на странице кластера" })).toBeNull();
    expect(screen.getByLabelText("Токен")).toHaveAttribute("placeholder", "Bearer-токен");
    expect(screen.queryByRole("alert")).toBeNull();
  });
});
