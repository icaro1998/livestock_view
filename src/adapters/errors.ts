export class AdapterError extends Error {
  status: number;
  body?: unknown;

  constructor(status: number, message: string, body?: unknown) {
    super(message);
    this.status = status;
    this.body = body;
  }
}

export const isAdapterError = (error: unknown): error is AdapterError =>
  error instanceof AdapterError;
