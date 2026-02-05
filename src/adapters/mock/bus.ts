import type { RealtimeEnvelope, RealtimeTopic } from "../../contract/types";

export class MockRealtimeBus {
  private handlers = new Set<(message: RealtimeEnvelope) => void>();

  emit(message: RealtimeEnvelope) {
    for (const handler of this.handlers) {
      handler(message);
    }
  }

  on(handler: (message: RealtimeEnvelope) => void) {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }
}

export type SubscriptionState = {
  topics: Set<RealtimeTopic>;
};
