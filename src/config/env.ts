export type DataAdapterKind = "MOCK" | "HTTP";
export type RealtimeAdapterKind = "MOCK" | "WS";

const normalizeDataAdapter = (value: string | undefined, fallback: DataAdapterKind) => {
  if (!value) return fallback;
  const upper = value.toUpperCase();
  return upper === "HTTP" || upper === "MOCK" ? (upper as DataAdapterKind) : fallback;
};

const normalizeRealtimeAdapter = (
  value: string | undefined,
  fallback: RealtimeAdapterKind
) => {
  if (!value) return fallback;
  const upper = value.toUpperCase();
  return upper === "WS" || upper === "MOCK" ? (upper as RealtimeAdapterKind) : fallback;
};

const apiUrl = import.meta.env.API_URL || import.meta.env.VITE_API_URL || "";
const wsUrl = import.meta.env.WS_URL || import.meta.env.VITE_WS_URL || "";

const dataAdapter = normalizeDataAdapter(
  import.meta.env.DATA_ADAPTER || import.meta.env.VITE_DATA_ADAPTER,
  "MOCK"
);

const realtimeAdapter = normalizeRealtimeAdapter(
  import.meta.env.REALTIME_ADAPTER || import.meta.env.VITE_REALTIME_ADAPTER,
  "MOCK"
);

const accessToken =
  import.meta.env.ACCESS_TOKEN || import.meta.env.VITE_ACCESS_TOKEN || "";

export const env = {
  apiUrl,
  wsUrl,
  dataAdapter,
  realtimeAdapter,
  accessToken
};
