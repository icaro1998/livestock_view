import { createContext, useContext, useEffect, useMemo } from "react";
import type { ReactNode } from "react";
import { createAdapters } from "./factory";
import type { IDataAdapter } from "./IDataAdapter";
import type { IRealtimeAdapter } from "./IRealtimeAdapter";
import type { RealtimeTopic } from "../contract/types";
import { contract } from "../contract/contract.generated";
import { handleRealtimeMessage } from "../state/realtime";

export type AdapterSet = {
  dataAdapter: IDataAdapter;
  realtimeAdapter: IRealtimeAdapter;
};

const AdapterContext = createContext<AdapterSet | null>(null);

export const AdaptersProvider = ({ children }: { children: ReactNode }) => {
  const adapters = useMemo(() => createAdapters(), []);

  useEffect(() => {
    const topics = Object.keys(contract.websocket.topics) as RealtimeTopic[];
    let unsubscribe = () => {};

    const connect = async () => {
      await adapters.realtimeAdapter.connect();
      adapters.realtimeAdapter.subscribe(topics);
      unsubscribe = adapters.realtimeAdapter.onMessage((message) => {
        void handleRealtimeMessage(message, adapters.dataAdapter);
      });
    };

    void connect();

    return () => {
      unsubscribe();
      void adapters.realtimeAdapter.disconnect();
    };
  }, [adapters]);

  return <AdapterContext.Provider value={adapters}>{children}</AdapterContext.Provider>;
};

export const useAdapters = () => {
  const context = useContext(AdapterContext);
  if (!context) {
    throw new Error("AdaptersProvider is missing");
  }
  return context;
};
