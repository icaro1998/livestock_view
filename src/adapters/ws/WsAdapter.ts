import type { IRealtimeAdapter } from "../IRealtimeAdapter";
import type { RealtimeEnvelope, RealtimeTopic } from "../../contract/types";

export class WsAdapter implements IRealtimeAdapter {
  private url: string;
  private accessToken?: string;
  private socket: WebSocket | null = null;
  private handlers = new Set<(message: RealtimeEnvelope) => void>();
  private subscriptions = new Set<RealtimeTopic>();

  constructor(options: { url: string; accessToken?: string }) {
    this.url = options.url;
    this.accessToken = options.accessToken;
  }

  async connect() {
    if (this.socket && this.socket.readyState === WebSocket.OPEN) return;
    const url = this.buildUrl();
    this.socket = new WebSocket(url);
    this.socket.addEventListener("message", (event) => {
      try {
        const message = JSON.parse(event.data) as RealtimeEnvelope;
        if (
          this.subscriptions.size === 0 ||
          (message?.topic && this.subscriptions.has(message.topic))
        ) {
          this.handlers.forEach((handler) => handler(message));
        }
      } catch {
        // Ignore non-JSON messages
      }
    });
  }

  async disconnect() {
    if (this.socket) {
      this.socket.close();
      this.socket = null;
    }
  }

  subscribe(topics: RealtimeTopic[]) {
    topics.forEach((topic) => this.subscriptions.add(topic));
  }

  unsubscribe(topics: RealtimeTopic[]) {
    topics.forEach((topic) => this.subscriptions.delete(topic));
  }

  onMessage(handler: (message: RealtimeEnvelope) => void) {
    this.handlers.add(handler);
    return () => this.handlers.delete(handler);
  }

  private buildUrl() {
    if (!this.accessToken) return this.url;
    const url = new URL(this.url, window.location.origin);
    if (!url.searchParams.has("token") && !url.searchParams.has("accessToken")) {
      url.searchParams.set("token", this.accessToken);
    }
    return url.toString();
  }
}
