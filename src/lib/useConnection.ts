import { useQuery } from "@tanstack/react-query";
import { api } from "../api/client";
import type { ConnectionProfile } from "../api/types";

/** Профиль подключения из общего кэша ["connections"]; undefined — пока грузится или удалён. */
export function useConnection(connectionId: string): ConnectionProfile | undefined {
  const { data } = useQuery({ queryKey: ["connections"], queryFn: api.listConnections });
  return data?.find((c) => c.id === connectionId);
}
