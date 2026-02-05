import type { IRealtimeAdapter } from "../IRealtimeAdapter";
import type { RealtimeEnvelope, RealtimeTopic } from "../../contract/types";
import { MockRealtimeBus, SubscriptionState } from "./bus";

export class MockRealtimeAdapter implements IRealtimeAdapter {
  private bus: MockRealtimeBus;
  private subscription: SubscriptionState;
  private connected = false;

  constructor(bus: MockRealtimeBus, subscription: SubscriptionState) {
    this.bus = bus;
    this.subscription = subscription;
  }

  async connect() {
    this.connected = true;
  }

  async disconnect() {
    this.connected = false;
  }

  subscribe(topics: RealtimeTopic[]) {
    for (const topic of topics) {
      this.subscription.topics.add(topic);
    }
  }

  unsubscribe(topics: RealtimeTopic[]) {
    for (const topic of topics) {
      this.subscription.topics.delete(topic);
    }
  }

  onMessage(handler: (message: RealtimeEnvelope) => void) {
    return this.bus.on((message) => {
      if (!this.connected) return;
      if (this.subscription.topics.size === 0 || this.subscription.topics.has(message.topic)) {
        handler(message);
      }
    });
  }
}
