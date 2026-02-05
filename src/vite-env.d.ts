/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly API_URL?: string;
  readonly WS_URL?: string;
  readonly DATA_ADAPTER?: string;
  readonly REALTIME_ADAPTER?: string;
  readonly ACCESS_TOKEN?: string;
  readonly VITE_API_URL?: string;
  readonly VITE_WS_URL?: string;
  readonly VITE_DATA_ADAPTER?: string;
  readonly VITE_REALTIME_ADAPTER?: string;
  readonly VITE_ACCESS_TOKEN?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
