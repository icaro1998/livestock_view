import type { IDataAdapter } from "../IDataAdapter";
import type {
  Animal,
  AnimalCreate,
  AnimalEventsListQuery,
  AnimalEventWithSubtypes,
  AnimalMetrics,
  AnimalUpdate,
  AnalyticsSummary,
  AnimalsListQuery,
  CostBulkCreate,
  CostCreate,
  CostEvent,
  CostsListQuery,
  Endpoint,
  EventBulkCreate,
  EventCreate,
  EventsListQuery,
  GroupCreate,
  HerdGroup,
  Location,
  LocationCreate,
  Paginated,
  Party,
  PartyCreate,
  Product,
  ProductCreate,
  Role
} from "../../contract/types";
import { endpointBy, dimensionFieldSupportByTable } from "../../contract/types";
import { roleAllows } from "../../contract/access";
import { AdapterError } from "../errors";

export class HttpAdapter implements IDataAdapter {
  private baseUrl: string;
  private accessToken?: string;
  private getRole: () => Role;

  constructor(options: { baseUrl: string; accessToken?: string; getRole: () => Role }) {
    this.baseUrl = options.baseUrl ?? "";
    this.accessToken = options.accessToken;
    this.getRole = options.getRole;
  }

  private assertRole(method: Endpoint["method"], path: Endpoint["path"]) {
    const endpoint = endpointBy(method, path);
    const required = endpoint.required_role;
    if (!required) return;
    const role = this.getRole();
    if (!roleAllows(role, required)) {
      throw new AdapterError(403, "Forbidden", { message: "Forbidden" });
    }
  }

  private buildUrl(path: string, query?: Record<string, unknown>) {
    const url = new URL(path, this.baseUrl || window.location.origin);
    if (query) {
      const params = new URLSearchParams();
      Object.entries(query).forEach(([key, value]) => {
        if (value === undefined || value === null || value === "") return;
        params.set(key, String(value));
      });
      const search = params.toString();
      if (search) url.search = search;
    }
    return url.toString();
  }

  private async request<T>(
    method: string,
    path: string,
    options?: { query?: Record<string, unknown>; body?: unknown; headers?: Record<string, string> }
  ): Promise<T> {
    const url = this.buildUrl(path, options?.query);
    const headers: Record<string, string> = {
      ...(options?.headers ?? {})
    };
    if (this.accessToken) {
      headers.Authorization = `Bearer ${this.accessToken}`;
    }
    if (options?.body) {
      headers["Content-Type"] = "application/json";
    }

    const response = await fetch(url, {
      method,
      headers,
      body: options?.body ? JSON.stringify(options.body) : undefined
    });

    const text = await response.text();
    const payload = text ? JSON.parse(text) : null;
    if (!response.ok) {
      throw new AdapterError(response.status, payload?.message ?? response.statusText, payload);
    }
    return payload as T;
  }

  async listAnimals(params: AnimalsListQuery): Promise<Paginated<Animal>> {
    this.assertRole("GET", "/animals");
    return this.request("GET", "/animals", { query: params });
  }

  async getAnimal(uid: string): Promise<Animal> {
    this.assertRole("GET", "/animals/:uid");
    return this.request("GET", `/animals/${encodeURIComponent(uid)}`);
  }

  async createAnimal(input: AnimalCreate): Promise<Animal> {
    this.assertRole("POST", "/animals");
    return this.request("POST", "/animals", { body: input });
  }

  async updateAnimal(uid: string, input: AnimalUpdate, options: { ifMatchVersion: number }): Promise<Animal> {
    this.assertRole("PATCH", "/animals/:uid");
    if (options.ifMatchVersion === undefined || options.ifMatchVersion === null) {
      throw new AdapterError(400, "if-match-version required", { message: "Validation error" });
    }
    return this.request("PATCH", `/animals/${encodeURIComponent(uid)}`, {
      body: input,
      headers: { "if-match-version": String(options.ifMatchVersion) }
    });
  }

  async listAnimalEvents(
    uid: string,
    params?: AnimalEventsListQuery
  ): Promise<Paginated<AnimalEventWithSubtypes>> {
    this.assertRole("GET", "/animals/:uid/events");
    return this.request("GET", `/animals/${encodeURIComponent(uid)}/events`, { query: params });
  }

  async listEvents(params?: EventsListQuery): Promise<Paginated<AnimalEventWithSubtypes>> {
    this.assertRole("GET", "/events");
    return this.request("GET", "/events", { query: params });
  }

  async createEvent(
    input: EventCreate,
    options?: { idempotencyKey?: string }
  ): Promise<AnimalEventWithSubtypes> {
    this.assertRole("POST", "/events");
    return this.request("POST", "/events", {
      body: input,
      headers: options?.idempotencyKey ? { "idempotency-key": options.idempotencyKey } : undefined
    });
  }

  async createEventsBulk(
    input: EventBulkCreate,
    options?: { idempotencyKey?: string }
  ): Promise<AnimalEventWithSubtypes[]> {
    this.assertRole("POST", "/events/bulk");
    return this.request("POST", "/events/bulk", {
      body: input,
      headers: options?.idempotencyKey ? { "idempotency-key": options.idempotencyKey } : undefined
    });
  }

  async listCosts(params?: CostsListQuery): Promise<Paginated<CostEvent>> {
    this.assertRole("GET", "/costs");
    return this.request("GET", "/costs", { query: params });
  }

  async createCost(input: CostCreate): Promise<CostEvent> {
    this.assertRole("POST", "/costs");
    return this.request("POST", "/costs", { body: input });
  }

  async createCostsBulk(input: CostBulkCreate): Promise<CostEvent[]> {
    this.assertRole("POST", "/costs/bulk");
    return this.request("POST", "/costs/bulk", { body: input });
  }

  async listLocations(): Promise<Location[]> {
    this.assertRole("GET", "/locations");
    return this.request("GET", "/locations");
  }

  async createLocation(input: LocationCreate): Promise<Location> {
    this.assertRole("POST", "/locations");
    return this.request("POST", "/locations", {
      body: pickFields(input, dimensionFieldSupportByTable.location)
    });
  }

  async listGroups(): Promise<HerdGroup[]> {
    this.assertRole("GET", "/groups");
    return this.request("GET", "/groups");
  }

  async createGroup(input: GroupCreate): Promise<HerdGroup> {
    this.assertRole("POST", "/groups");
    return this.request("POST", "/groups", {
      body: pickFields(input, dimensionFieldSupportByTable.herdGroup)
    });
  }

  async listParties(): Promise<Party[]> {
    this.assertRole("GET", "/parties");
    return this.request("GET", "/parties");
  }

  async createParty(input: PartyCreate): Promise<Party> {
    this.assertRole("POST", "/parties");
    return this.request("POST", "/parties", {
      body: pickFields(input, dimensionFieldSupportByTable.party)
    });
  }

  async listProducts(): Promise<Product[]> {
    this.assertRole("GET", "/products");
    return this.request("GET", "/products");
  }

  async createProduct(input: ProductCreate): Promise<Product> {
    this.assertRole("POST", "/products");
    return this.request("POST", "/products", {
      body: pickFields(input, dimensionFieldSupportByTable.product)
    });
  }

  async getAnalyticsSummary(): Promise<AnalyticsSummary> {
    this.assertRole("GET", "/dashboards/summary");
    return this.request("GET", "/dashboards/summary");
  }

  async getAnimalMetrics(uid: string): Promise<AnimalMetrics> {
    this.assertRole("GET", "/animals/:uid/metrics");
    return this.request("GET", `/animals/${encodeURIComponent(uid)}/metrics`);
  }
}

const pickFields = <T extends Record<string, unknown>, K extends keyof T>(
  input: T,
  allowed: readonly K[]
) => {
  const output = {} as Pick<T, K>;
  for (const key of allowed) {
    if (key in input) {
      output[key] = input[key];
    }
  }
  return output;
};
