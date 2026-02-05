import type { IDataAdapter } from "../IDataAdapter";
import type {
  Animal,
  AnimalCreate,
  AnimalEventsListQuery,
  AnimalEventWithSubtypes,
  AnimalUpdate,
  AnimalsListQuery,
  CostBulkCreate,
  CostCreate,
  CostEvent,
  CostsListQuery,
  EventBulkCreate,
  EventCreate,
  EventsListQuery,
  Endpoint,
  GroupCreate,
  HerdGroup,
  Location,
  LocationCreate,
  Party,
  PartyCreate,
  Product,
  ProductCreate,
  Role
} from "../../contract/types";
import {
  contract
} from "../../contract/contract.generated";
import {
  costCategoryValues,
  costScopeValues,
  dimensionFieldSupportByTable,
  endpointBy,
  eventTypeValues
} from "../../contract/types";
import { roleAllows } from "../../contract/access";
import { AdapterError } from "../errors";
import { MockRealtimeBus } from "./bus";

const nowIso = () => new Date().toISOString();

const toDecimalString = (value: number | string | undefined | null) => {
  if (value === undefined || value === null) return null;
  return typeof value === "string" ? value : String(value);
};

type DimensionBucket<T extends { id?: string | null; code?: string | null }> = {
  byCode: Map<string, T>;
  byId: Map<string, T>;
  nextId: number;
};

type MockState = {
  animals: Map<string, Animal>;
  events: Map<string, AnimalEventWithSubtypes>;
  eventOrder: string[];
  costs: Map<string, CostEvent>;
  costOrder: string[];
  dimensions: {
    location: DimensionBucket<Location>;
    herdGroup: DimensionBucket<HerdGroup>;
    party: DimensionBucket<Party>;
    product: DimensionBucket<Product>;
  };
  nextEventId: number;
  nextCostId: number;
};

const createDimensionBucket = <T extends { id?: string | null; code?: string | null }>(): DimensionBucket<T> => ({
  byCode: new Map<string, T>(),
  byId: new Map<string, T>(),
  nextId: 1
});

export const createMockState = (): MockState => {
  const state: MockState = {
    animals: new Map(),
    events: new Map(),
    eventOrder: [],
    costs: new Map(),
    costOrder: [],
    dimensions: {
      location: createDimensionBucket<Location>(),
      herdGroup: createDimensionBucket<HerdGroup>(),
      party: createDimensionBucket<Party>(),
      product: createDimensionBucket<Product>()
    },
    nextEventId: 102,
    nextCostId: 502
  };

  const exampleAnimal = contract.examples.animal_create?.response as Animal | undefined;
  if (exampleAnimal?.uid) {
    state.animals.set(exampleAnimal.uid, {
      alert: false,
      version: 1,
      created_at: nowIso(),
      updated_at: nowIso(),
      ...exampleAnimal
    });
  }

  const exampleDimension = contract.examples.dimension_create?.response as Location | undefined;
  if (exampleDimension?.code) {
    const location = {
      id: exampleDimension.id ?? String(state.dimensions.location.nextId++),
      code: exampleDimension.code,
      name: exampleDimension.name ?? null,
      type: exampleDimension.type ?? null,
      meta: exampleDimension.meta ?? null,
      created_at: nowIso(),
      updated_at: nowIso()
    };
    state.dimensions.location.byCode.set(location.code, location);
    if (location.id) state.dimensions.location.byId.set(location.id, location);
  }

  const exampleEvent = contract.examples.event_create_weight?.response as
    | AnimalEventWithSubtypes
    | undefined;
  if (exampleEvent?.event_id) {
    const event = {
      created_at: nowIso(),
      ...exampleEvent
    } as AnimalEventWithSubtypes;
    state.events.set(event.event_id, event);
    state.eventOrder.push(event.event_id);
  }

  const exampleCost = contract.examples.cost_create?.response as CostEvent | undefined;
  if (exampleCost?.cost_id) {
    const cost = {
      created_at: nowIso(),
      currency: exampleCost.currency ?? "BOB",
      ...exampleCost
    } as CostEvent;
    state.costs.set(cost.cost_id, cost);
    state.costOrder.push(cost.cost_id);
  }

  return state;
};

const ensureDimensionRecord = (
  table: keyof MockState["dimensions"],
  state: MockState,
  code: string
) => {
  const bucket = state.dimensions[table] as DimensionBucket<any>;
  const existing = bucket.byCode.get(code);
  if (existing) return existing;
  const id = String(bucket.nextId++);
  const timestamp = nowIso();

  let record: any = { id, code, created_at: timestamp, updated_at: timestamp };
  if (table === "location") {
    record = { ...record, name: null, type: null, meta: null };
  }
  if (table === "herdGroup") {
    record = { ...record, name: null, meta: null };
  }
  if (table === "party") {
    record = { ...record, name: null, type: null, meta: null };
  }
  if (table === "product") {
    record = { ...record, name: null, category: null, unit: null, meta: null };
  }

  bucket.byCode.set(code, record);
  bucket.byId.set(id, record);
  return record;
};

const upsertDimensionRecord = <T extends { id?: string | null; code?: string | null }>(
  bucket: DimensionBucket<T>,
  record: T
) => {
  if (record.code) {
    bucket.byCode.set(record.code, record);
  }
  if (record.id) {
    bucket.byId.set(record.id, record);
  }
};

const sortByDateDesc = <T>(items: T[], getDate: (item: T) => string | undefined | null) =>
  items.sort((a, b) => {
    const aTime = Date.parse(getDate(a) ?? "");
    const bTime = Date.parse(getDate(b) ?? "");
    return bTime - aTime;
  });

const getLastWeight = (events: AnimalEventWithSubtypes[]) => {
  const weightEvents = events.filter((event) => event.event_type === "weight");
  if (weightEvents.length === 0) return null;
  const sorted = sortByDateDesc(weightEvents, (event) => event.event_at);
  const latest = sorted[0];
  const weightKg = latest.weight?.weight_kg ?? null;
  return weightKg;
};

export class MockDataAdapter implements IDataAdapter {
  private state: MockState;
  private bus: MockRealtimeBus;
  private getRole: () => Role;

  constructor(
    state: MockState,
    bus: MockRealtimeBus,
    getRole: () => Role
  ) {
    this.state = state;
    this.bus = bus;
    this.getRole = getRole;
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

  async listAnimals(params: AnimalsListQuery) {
    this.assertRole("GET", "/animals");
    const animals = Array.from(this.state.animals.values());

    let filtered = animals;
    if (params?.search) {
      const search = params.search.toLowerCase();
      filtered = filtered.filter((animal) => {
        const haystack = [animal.uid, animal.eid, animal.vid, animal.race, animal.brand_mark]
          .filter(Boolean)
          .join(" ")
          .toLowerCase();
        return haystack.includes(search);
      });
    }

    if (params?.brand_mark) {
      filtered = filtered.filter((animal) => animal.brand_mark === params.brand_mark);
    }

    if (params?.min_age_months !== undefined || params?.max_age_months !== undefined) {
      const now = new Date();
      filtered = filtered.filter((animal) => {
        if (!animal.birth_year) return false;
        const birthMonth = animal.birth_month ?? 1;
        const birthDate = new Date(animal.birth_year, birthMonth - 1, 1);
        const diffMonths =
          (now.getFullYear() - birthDate.getFullYear()) * 12 +
          (now.getMonth() - birthDate.getMonth());
        if (params.min_age_months !== undefined && diffMonths < params.min_age_months) return false;
        if (params.max_age_months !== undefined && diffMonths > params.max_age_months) return false;
        return true;
      });
    }

    if (params?.min_weight !== undefined || params?.max_weight !== undefined) {
      filtered = filtered.filter((animal) => {
        const events = Array.from(this.state.events.values()).filter((event) => event.uid === animal.uid);
        const lastWeight = getLastWeight(events);
        if (!lastWeight) return false;
        const numericWeight = Number(lastWeight);
        if (Number.isNaN(numericWeight)) return false;
        if (params.min_weight !== undefined && numericWeight < params.min_weight) return false;
        if (params.max_weight !== undefined && numericWeight > params.max_weight) return false;
        return true;
      });
    }

    if (params?.last_event_type) {
      filtered = filtered.filter((animal) => {
        const events = Array.from(this.state.events.values()).filter((event) => event.uid === animal.uid);
        if (events.length === 0) return false;
        const latest = sortByDateDesc(events, (event) => event.event_at)[0];
        return latest?.event_type === params.last_event_type;
      });
    }

    if (params?.location_code) {
      const location = this.state.dimensions.location.byCode.get(params.location_code);
      if (location?.id) {
        filtered = filtered.filter((animal) => {
          const events = Array.from(this.state.events.values()).filter((event) => event.uid === animal.uid);
          const movement = sortByDateDesc(
            events.filter((event) => event.event_type === "movement"),
            (event) => event.event_at
          )[0];
          return movement?.location_to_id === location.id || movement?.location_from_id === location.id;
        });
      }
    }

    if (params?.from || params?.to) {
      filtered = filtered.filter((animal) => {
        if (!animal.registration_at) return false;
        const reg = Date.parse(animal.registration_at);
        if (params.from && reg < Date.parse(params.from)) return false;
        if (params.to && reg > Date.parse(params.to)) return false;
        return true;
      });
    }

    const limit = params?.limit ?? 50;
    const offset = params?.cursor ? Number(params.cursor) : 0;
    const slice = filtered.slice(offset, offset + limit);
    const nextCursor = offset + limit < filtered.length ? String(offset + limit) : null;

    return { data: slice, nextCursor };
  }

  async getAnimal(uid: string) {
    this.assertRole("GET", "/animals/:uid");
    const animal = this.state.animals.get(uid);
    if (!animal) throw new AdapterError(404, "Animal not found", { message: "Not Found" });
    return animal;
  }

  async createAnimal(input: AnimalCreate) {
    this.assertRole("POST", "/animals");
    if (!input.uid) throw new AdapterError(400, "Validation error", { message: "uid required" });
    const now = nowIso();
    const existing = this.state.animals.get(input.uid);
    if (existing) return existing;
    const animal: Animal = {
      uid: input.uid,
      alert: input.alert ?? false,
      version: 1,
      created_at: now,
      updated_at: now,
      ...input
    };
    this.state.animals.set(animal.uid, animal);

    this.bus.emit({
      topic: "animal.updated",
      ts: now,
      data: { uid: animal.uid }
    });

    return animal;
  }

  async updateAnimal(uid: string, input: AnimalUpdate, options: { ifMatchVersion: number }) {
    this.assertRole("PATCH", "/animals/:uid");
    if (options.ifMatchVersion === undefined || options.ifMatchVersion === null) {
      throw new AdapterError(400, "if-match-version required", { message: "Validation error" });
    }
    const existing = this.state.animals.get(uid);
    if (!existing) throw new AdapterError(404, "Animal not found", { message: "Not Found" });
    if (existing.version !== options.ifMatchVersion) {
      throw new AdapterError(409, "Version mismatch", {
        message: "Version mismatch",
        details: { currentVersion: existing.version }
      });
    }
    const updated: Animal = {
      ...existing,
      ...input,
      updated_at: nowIso(),
      version: existing.version + 1
    };
    this.state.animals.set(uid, updated);

    this.bus.emit({
      topic: "animal.updated",
      ts: nowIso(),
      data: { uid: updated.uid, version: updated.version }
    });

    return updated;
  }

  async listAnimalEvents(uid: string, params?: AnimalEventsListQuery) {
    this.assertRole("GET", "/animals/:uid/events");
    return this.listEvents({ uid, ...params });
  }

  async listEvents(params?: EventsListQuery) {
    this.assertRole("GET", "/events");
    const events = Array.from(this.state.events.values());
    let filtered = events;
    if (params?.uid) filtered = filtered.filter((event) => event.uid === params.uid);
    if (params?.event_type) filtered = filtered.filter((event) => event.event_type === params.event_type);
    if (params?.batch_id) filtered = filtered.filter((event) => event.batch_id === params.batch_id);

    if (params?.location_code) {
      const location = this.state.dimensions.location.byCode.get(params.location_code);
      if (location?.id) {
        filtered = filtered.filter(
          (event) => event.location_from_id === location.id || event.location_to_id === location.id
        );
      }
    }

    if (params?.group_code) {
      const group = this.state.dimensions.herdGroup.byCode.get(params.group_code);
      if (group?.id) {
        filtered = filtered.filter((event) => event.group_id === group.id);
      }
    }

    if (params?.from || params?.to) {
      filtered = filtered.filter((event) => {
        const time = Date.parse(event.event_at);
        if (params.from && time < Date.parse(params.from)) return false;
        if (params.to && time > Date.parse(params.to)) return false;
        return true;
      });
    }

    const sorted = sortByDateDesc(filtered, (event) => event.event_at);
    const limit = params?.limit ?? 50;
    const offset = params?.cursor ? Number(params.cursor) : 0;
    const slice = sorted.slice(offset, offset + limit);
    const nextCursor = offset + limit < sorted.length ? String(offset + limit) : null;

    return { data: slice, nextCursor };
  }

  async createEvent(input: EventCreate, options?: { idempotencyKey?: string }) {
    this.assertRole("POST", "/events");
    if (!input.uid || !input.event_at || !input.event_type) {
      throw new AdapterError(400, "Validation error", { message: "uid, event_at, event_type required" });
    }
    if (!eventTypeValues.includes(input.event_type as any)) {
      throw new AdapterError(400, "Validation error", { message: "invalid event_type" });
    }
    if (
      input.event_type === "movement" &&
      (!input.location_from_code || !input.location_to_code)
    ) {
      throw new AdapterError(400, "Validation error", {
        message: "movement events require location_from_code and location_to_code"
      });
    }

    const sourceRef = options?.idempotencyKey ?? input.source_ref ?? null;
    const dedupeKey = [
      input.uid,
      input.event_at,
      input.event_type,
      input.event_subtype ?? "",
      sourceRef ?? ""
    ].join("|");

    const existing = Array.from(this.state.events.values()).find((event) => {
      const key = [
        event.uid,
        event.event_at,
        event.event_type,
        event.event_subtype ?? "",
        event.source_ref ?? ""
      ].join("|");
      return key === dedupeKey;
    });

    if (existing) return existing;

    const eventId = String(this.state.nextEventId++);
    const createdAt = nowIso();

    const locationFrom = input.location_from_code
      ? ensureDimensionRecord("location", this.state, input.location_from_code)
      : null;
    const locationTo = input.location_to_code
      ? ensureDimensionRecord("location", this.state, input.location_to_code)
      : null;
    const group = input.group_code
      ? ensureDimensionRecord("herdGroup", this.state, input.group_code)
      : null;
    const party = input.party_code
      ? ensureDimensionRecord("party", this.state, input.party_code)
      : null;
    const product = input.product_code
      ? ensureDimensionRecord("product", this.state, input.product_code)
      : null;

    const event: AnimalEventWithSubtypes = {
      event_id: eventId,
      uid: input.uid,
      event_at: input.event_at,
      event_type: input.event_type,
      event_subtype: input.event_subtype ?? null,
      source_ref: sourceRef,
      batch_id: input.batch_id ?? null,
      confidence: toDecimalString(input.confidence),
      notes: input.notes ?? null,
      location_from_id: locationFrom?.id ?? null,
      location_to_id: locationTo?.id ?? null,
      group_id: group?.id ?? null,
      party_id: party?.id ?? null,
      product_id: product?.id ?? null,
      payload: input.payload ?? null,
      created_at: createdAt
    };

    if (input.event_type === "weight") {
      event.weight = {
        event_id: eventId,
        weight_kg: toDecimalString(input.weight_kg),
        method: input.method ?? null,
        shrink_pct: toDecimalString(input.shrink_pct)
      };
    }

    if (input.event_type === "movement") {
      const transportParty = input.transport_party_code
        ? ensureDimensionRecord("party", this.state, input.transport_party_code)
        : null;
      event.movement = {
        event_id: eventId,
        reason: input.reason ?? null,
        distance_km: toDecimalString(input.distance_km),
        transport_party_id: transportParty?.id ?? null
      };
    }

    if (input.event_type === "repro") {
      event.repro = {
        event_id: eventId,
        repro_action: input.repro_action ?? null,
        sire_uid: input.sire_uid ?? null,
        dam_uid: input.dam_uid ?? null,
        result: input.result ?? null,
        calf_uid: input.calf_uid ?? null,
        gestation_days: input.gestation_days ?? null
      };
    }

    if (input.event_type === "health") {
      event.health = {
        event_id: eventId,
        action: input.action ?? null,
        diagnosis: input.diagnosis ?? null,
        dose: toDecimalString(input.dose),
        dose_unit: input.dose_unit ?? null,
        withdrawal_days: input.withdrawal_days ?? null
      };
    }

    if (input.event_type === "nutrition") {
      event.nutrition = {
        event_id: eventId,
        ration_code: input.ration_code ?? null,
        intake_kg_day: toDecimalString(input.intake_kg_day),
        supplement_code: input.supplement_code ?? null,
        reason: input.reason ?? null
      };
    }

    this.state.events.set(event.event_id, event);
    this.state.eventOrder.push(event.event_id);

    this.bus.emit({
      topic: "event.created",
      ts: createdAt,
      data: event
    });

    if (locationFrom?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "location", code: locationFrom.code }
      });
    }
    if (locationTo?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "location", code: locationTo.code }
      });
    }
    if (group?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "herdGroup", code: group.code }
      });
    }
    if (party?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "party", code: party.code }
      });
    }
    if (product?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "product", code: product.code }
      });
    }

    return event;
  }

  async createEventsBulk(input: EventBulkCreate, options?: { idempotencyKey?: string }) {
    this.assertRole("POST", "/events/bulk");
    if (!input.events || !Array.isArray(input.events)) {
      throw new AdapterError(400, "Validation error", { message: "events required" });
    }

    const results: AnimalEventWithSubtypes[] = [];
    for (const eventInput of input.events) {
      const created = await this.createEvent(eventInput, options);
      results.push(created);
    }
    return results;
  }

  async listCosts(params?: CostsListQuery) {
    this.assertRole("GET", "/costs");
    let costs = Array.from(this.state.costs.values());

    if (params?.scope) costs = costs.filter((cost) => cost.scope === params.scope);
    if (params?.uid) costs = costs.filter((cost) => cost.uid === params.uid);
    if (params?.category) costs = costs.filter((cost) => cost.category === params.category);
    if (params?.batch_id) costs = costs.filter((cost) => cost.batch_id === params.batch_id);

    if (params?.from || params?.to) {
      costs = costs.filter((cost) => {
        const time = Date.parse(cost.cost_at);
        if (params.from && time < Date.parse(params.from)) return false;
        if (params.to && time > Date.parse(params.to)) return false;
        return true;
      });
    }

    const sorted = sortByDateDesc(costs, (cost) => cost.cost_at);
    const limit = params?.limit ?? 50;
    const offset = params?.cursor ? Number(params.cursor) : 0;
    const slice = sorted.slice(offset, offset + limit);
    const nextCursor = offset + limit < sorted.length ? String(offset + limit) : null;

    return { data: slice, nextCursor };
  }

  async createCost(input: CostCreate) {
    this.assertRole("POST", "/costs");
    if (!input.cost_at || !input.scope || !input.category || input.amount === undefined) {
      throw new AdapterError(400, "Validation error", {
        message: "cost_at, scope, category, amount required"
      });
    }
    if (!costScopeValues.includes(input.scope as any)) {
      throw new AdapterError(400, "Validation error", { message: "invalid scope" });
    }
    if (!costCategoryValues.includes(input.category as any)) {
      throw new AdapterError(400, "Validation error", { message: "invalid category" });
    }

    const location = input.location_code
      ? ensureDimensionRecord("location", this.state, input.location_code)
      : null;
    const group = input.group_code
      ? ensureDimensionRecord("herdGroup", this.state, input.group_code)
      : null;
    const party = input.party_code
      ? ensureDimensionRecord("party", this.state, input.party_code)
      : null;
    const product = input.product_code
      ? ensureDimensionRecord("product", this.state, input.product_code)
      : null;

    const costId = String(this.state.nextCostId++);
    const createdAt = nowIso();

    const cost: CostEvent = {
      cost_id: costId,
      cost_at: input.cost_at,
      scope: input.scope,
      uid: input.uid ?? null,
      group_id: group?.id ?? null,
      location_id: location?.id ?? null,
      category: input.category,
      product_id: product?.id ?? null,
      party_id: party?.id ?? null,
      amount: toDecimalString(input.amount) ?? "0",
      currency: input.currency ?? "BOB",
      quantity: toDecimalString(input.quantity),
      unit: input.unit ?? null,
      source_ref: input.source_ref ?? null,
      batch_id: input.batch_id ?? null,
      notes: input.notes ?? null,
      created_at: createdAt,
      event_id: null
    };

    this.state.costs.set(cost.cost_id, cost);
    this.state.costOrder.push(cost.cost_id);

    this.bus.emit({
      topic: "cost.created",
      ts: createdAt,
      data: cost
    });

    if (location?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "location", code: location.code }
      });
    }
    if (group?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "herdGroup", code: group.code }
      });
    }
    if (party?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "party", code: party.code }
      });
    }
    if (product?.code) {
      this.bus.emit({
        topic: "dimension.updated",
        ts: createdAt,
        data: { table: "product", code: product.code }
      });
    }

    return cost;
  }

  async createCostsBulk(input: CostBulkCreate) {
    this.assertRole("POST", "/costs/bulk");
    if (!input.costs || !Array.isArray(input.costs)) {
      throw new AdapterError(400, "Validation error", { message: "costs required" });
    }
    const results: CostEvent[] = [];
    for (const costInput of input.costs) {
      const created = await this.createCost(costInput);
      results.push(created);
    }
    return results;
  }

  async listLocations() {
    this.assertRole("GET", "/locations");
    return Array.from(this.state.dimensions.location.byCode.values());
  }

  async createLocation(input: LocationCreate) {
    this.assertRole("POST", "/locations");
    if (!input.code) throw new AdapterError(400, "Validation error", { message: "code required" });

    const allowed = dimensionFieldSupportByTable.location;
    const payload = pickFields(input, allowed);

    const bucket = this.state.dimensions.location;
    const id = String(bucket.nextId++);
    const now = nowIso();
    const record: Location = {
      id,
      code: payload.code,
      name: payload.name ?? null,
      type: payload.type ?? null,
      meta: payload.meta ?? null,
      created_at: now,
      updated_at: now
    };
    upsertDimensionRecord(bucket, record);

    this.bus.emit({
      topic: "dimension.updated",
      ts: now,
      data: { table: "location", code: record.code }
    });

    return record;
  }

  async listGroups() {
    this.assertRole("GET", "/groups");
    return Array.from(this.state.dimensions.herdGroup.byCode.values());
  }

  async createGroup(input: GroupCreate) {
    this.assertRole("POST", "/groups");
    if (!input.code) throw new AdapterError(400, "Validation error", { message: "code required" });

    const allowed = dimensionFieldSupportByTable.herdGroup;
    const payload = pickFields(input, allowed);

    const bucket = this.state.dimensions.herdGroup;
    const id = String(bucket.nextId++);
    const now = nowIso();
    const record: HerdGroup = {
      id,
      code: payload.code,
      name: payload.name ?? null,
      meta: payload.meta ?? null,
      created_at: now,
      updated_at: now
    };
    upsertDimensionRecord(bucket, record);

    this.bus.emit({
      topic: "dimension.updated",
      ts: now,
      data: { table: "herdGroup", code: record.code }
    });

    return record;
  }

  async listParties() {
    this.assertRole("GET", "/parties");
    return Array.from(this.state.dimensions.party.byCode.values());
  }

  async createParty(input: PartyCreate) {
    this.assertRole("POST", "/parties");
    if (!input.code) throw new AdapterError(400, "Validation error", { message: "code required" });

    const allowed = dimensionFieldSupportByTable.party;
    const payload = pickFields(input, allowed);

    const bucket = this.state.dimensions.party;
    const id = String(bucket.nextId++);
    const now = nowIso();
    const record: Party = {
      id,
      code: payload.code,
      name: payload.name ?? null,
      type: payload.type ?? null,
      meta: payload.meta ?? null,
      created_at: now,
      updated_at: now
    };
    upsertDimensionRecord(bucket, record);

    this.bus.emit({
      topic: "dimension.updated",
      ts: now,
      data: { table: "party", code: record.code }
    });

    return record;
  }

  async listProducts() {
    this.assertRole("GET", "/products");
    return Array.from(this.state.dimensions.product.byCode.values());
  }

  async createProduct(input: ProductCreate) {
    this.assertRole("POST", "/products");
    if (!input.code) throw new AdapterError(400, "Validation error", { message: "code required" });

    const allowed = dimensionFieldSupportByTable.product;
    const payload = pickFields(input, allowed);

    const bucket = this.state.dimensions.product;
    const id = String(bucket.nextId++);
    const now = nowIso();
    const record: Product = {
      id,
      code: payload.code,
      name: payload.name ?? null,
      category: payload.category ?? null,
      unit: payload.unit ?? null,
      meta: payload.meta ?? null,
      created_at: now,
      updated_at: now
    };
    upsertDimensionRecord(bucket, record);

    this.bus.emit({
      topic: "dimension.updated",
      ts: now,
      data: { table: "product", code: record.code }
    });

    return record;
  }

  async getAnalyticsSummary() {
    this.assertRole("GET", "/dashboards/summary");
    return contract.examples.analytics_summary?.response ?? {};
  }

  async getAnimalMetrics(uid: string) {
    this.assertRole("GET", "/animals/:uid/metrics");
    const events = Array.from(this.state.events.values()).filter((event) => event.uid === uid);
    const sorted = sortByDateDesc(events, (event) => event.event_at);
    const lastWeight = sorted.find((event) => event.event_type === "weight");
    const prevWeight = sorted.filter((event) => event.event_type === "weight")[1];

    const lastWeightKg = lastWeight?.weight?.weight_kg ?? null;
    const prevWeightKg = prevWeight?.weight?.weight_kg ?? null;

    let adg = null as number | null;
    if (lastWeightKg && prevWeightKg && lastWeight?.event_at && prevWeight?.event_at) {
      const delta = Number(lastWeightKg) - Number(prevWeightKg);
      const days =
        (Date.parse(lastWeight.event_at) - Date.parse(prevWeight.event_at)) / (1000 * 60 * 60 * 24);
      if (days > 0 && Number.isFinite(delta)) {
        adg = Number((delta / days).toFixed(2));
      }
    }

    return {
      adg,
      last_weight: lastWeightKg,
      last_weight_at: lastWeight?.event_at ?? null,
      prev_weight: prevWeightKg,
      prev_weight_at: prevWeight?.event_at ?? null,
      last_location_id: sorted.find((event) => event.event_type === "movement")?.location_to_id ?? null
    };
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
