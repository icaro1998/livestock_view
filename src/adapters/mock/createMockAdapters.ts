import { MockDataAdapter, createMockState } from "./MockDataAdapter";
import { MockRealtimeAdapter } from "./MockRealtimeAdapter";
import { MockRealtimeBus } from "./bus";
import type { RealtimeTopic, Role } from "../../contract/types";

export const createMockAdapters = ({ getRole }: { getRole: () => Role }) => {
  const bus = new MockRealtimeBus();
  const subscription = { topics: new Set<RealtimeTopic>() };
  const state = createMockState();

  return {
    data: new MockDataAdapter(state, bus, getRole),
    realtime: new MockRealtimeAdapter(bus, subscription)
  };
};
