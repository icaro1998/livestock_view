import type { RealtimeEnvelope, RealtimeTopic } from "../contract/types";

export interface IRealtimeAdapter {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  subscribe(topics: RealtimeTopic[]): void;
  unsubscribe(topics: RealtimeTopic[]): void;
  onMessage(handler: (message: RealtimeEnvelope) => void): () => void;
}
