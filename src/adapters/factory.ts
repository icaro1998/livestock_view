import { env } from "../config/env";
import { HttpAdapter } from "./http/HttpAdapter";
import { WsAdapter } from "./ws/WsAdapter";
import { createMockAdapters } from "./mock/createMockAdapters";
import { useAppStore } from "../state/store";

const resolveApiUrl = () => env.apiUrl || window.location.origin;

const resolveWsUrl = () => {
  if (env.wsUrl) return env.wsUrl;
  const base = window.location.origin.replace(/^http/, "ws");
  return new URL("/ws", base).toString();
};

export const createAdapters = () => {
  const getRole = () => useAppStore.getState().role;
  const useMock = env.dataAdapter === "MOCK" || env.realtimeAdapter === "MOCK";
  const mock = useMock ? createMockAdapters({ getRole }) : null;

  const dataAdapter =
    env.dataAdapter === "HTTP"
      ? new HttpAdapter({
          baseUrl: resolveApiUrl(),
          accessToken: env.accessToken || undefined,
          getRole
        })
      : mock!.data;

  const realtimeAdapter =
    env.realtimeAdapter === "WS"
      ? new WsAdapter({ url: resolveWsUrl(), accessToken: env.accessToken || undefined })
      : mock!.realtime;

  return { dataAdapter, realtimeAdapter };
};
