import type { IDataAdapter } from "../adapters/IDataAdapter";
import type { RealtimeEnvelope } from "../contract/types";
import { useAppStore } from "./store";

const isRecord = (value: unknown): value is Record<string, any> =>
  typeof value === "object" && value !== null;

export const handleRealtimeMessage = async (message: RealtimeEnvelope, adapter: IDataAdapter) => {
  const { actions } = useAppStore.getState();

  switch (message.topic) {
    case "animal.updated": {
      if (!isRecord(message.data) || typeof message.data.uid !== "string") return;
      const animal = await adapter.getAnimal(message.data.uid);
      actions.upsertAnimals([animal]);
      return;
    }
    case "event.created": {
      if (!isRecord(message.data)) return;
      actions.upsertEvents([message.data as any]);
      return;
    }
    case "cost.created": {
      if (!isRecord(message.data)) return;
      actions.upsertCosts([message.data as any]);
      return;
    }
    case "dimension.updated": {
      if (!isRecord(message.data) || typeof message.data.table !== "string") return;
      if (message.data.table === "location") {
        const locations = await adapter.listLocations();
        actions.upsertDimensions("locations", locations);
      }
      if (message.data.table === "herdGroup") {
        const groups = await adapter.listGroups();
        actions.upsertDimensions("groups", groups);
      }
      if (message.data.table === "party") {
        const parties = await adapter.listParties();
        actions.upsertDimensions("parties", parties);
      }
      if (message.data.table === "product") {
        const products = await adapter.listProducts();
        actions.upsertDimensions("products", products);
      }
      return;
    }
    default:
      return;
  }
};
