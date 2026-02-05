import { contract } from "./contract.generated";

export type Contract = typeof contract;

export type Role = Contract["roles_acl"]["roles"][number];
export type RoleHierarchy = Contract["roles_acl"]["hierarchy"];

export type Endpoint = Contract["rest_api"]["endpoints"][number];

export type EndpointFor<M extends Endpoint["method"], P extends Endpoint["path"]> = Extract<
  Endpoint,
  { method: M; path: P }
>;

export const endpointBy = <M extends Endpoint["method"], P extends Endpoint["path"]>(
  method: M,
  path: P
): EndpointFor<M, P> => {
  const match = contract.rest_api.endpoints.find(
    (endpoint) => endpoint.method === method && endpoint.path === path
  );
  if (!match) {
    throw new Error(`Endpoint ${method} ${path} not found in contract`);
  }
  return match as EndpointFor<M, P>;
};

const postEvents = endpointBy("POST", "/events");
const postCosts = endpointBy("POST", "/costs");

export const eventTypeValues = postEvents.request_schema.body.properties.event_type.enum;
export type EventType = (typeof eventTypeValues)[number];

export const costScopeValues = postCosts.request_schema.body.properties.scope.enum;
export type CostScope = (typeof costScopeValues)[number];

export const costCategoryValues = postCosts.request_schema.body.properties.category.enum;
export type CostCategory = (typeof costCategoryValues)[number];

export type DimensionTable = keyof Contract["dimensions"]["tables"];
export const dimensionTables = Object.keys(
  contract.dimensions.tables
) as DimensionTable[];
export const dimensionFieldSupportByTable = contract.dimensions.field_support_by_table;

export type RealtimeTopic = keyof Contract["websocket"]["topics"];

export type RealtimeEnvelope = {
  topic: RealtimeTopic;
  ts: string;
  requestId?: string;
  data: unknown;
};

export type SchemaType<S> = S extends { enum: readonly (infer E)[] }
  ? E
  : S extends { type: readonly (infer T)[] }
    ? T extends unknown
      ? SchemaType<{ type: T } & S>
      : never
    : S extends { type: "string" }
      ? string
      : S extends { type: "integer" | "number" }
        ? number
        : S extends { type: "boolean" }
          ? boolean
          : S extends { type: "null" }
            ? null
            : S extends { type: "array"; items: infer I }
              ? SchemaType<I>[]
              : S extends { type: "object" }
                ? SchemaObjectType<S>
                : unknown;

type SchemaObjectType<S> = S extends { properties: infer P }
  ? {
      [K in RequiredKeys<S> & keyof P]: SchemaType<P[K]>;
    } & {
      [K in Exclude<keyof P, RequiredKeys<S>> & keyof P]?: SchemaType<P[K]>;
    } & (S extends { additionalProperties: true } ? Record<string, unknown> : unknown)
  : Record<string, unknown>;

type RequiredKeys<S> = S extends { required: readonly (infer R)[] } ? R & string : never;

type ModelFields = Record<string, { type: string; optional?: boolean }>;

export type ModelByName<N extends keyof Contract["data_model"]["models"]> = ModelType<
  Contract["data_model"]["models"][N]["fields"]
>;

export type Animal = ModelByName<"Animal">;
export type Location = ModelByName<"Location">;
export type HerdGroup = ModelByName<"HerdGroup">;
export type Party = ModelByName<"Party">;
export type Product = ModelByName<"Product">;
export type AnimalEvent = ModelByName<"AnimalEvent">;
export type WeightEvent = ModelByName<"WeightEvent">;
export type MovementEvent = ModelByName<"MovementEvent">;
export type ReproEvent = ModelByName<"ReproEvent">;
export type HealthEvent = ModelByName<"HealthEvent">;
export type NutritionEvent = ModelByName<"NutritionEvent">;
export type CostEvent = ModelByName<"CostEvent">;
export type DerivedMetric = ModelByName<"DerivedMetric">;

export type AnimalEventWithSubtypes = AnimalEvent & {
  weight?: WeightEvent | null;
  movement?: MovementEvent | null;
  repro?: ReproEvent | null;
  health?: HealthEvent | null;
  nutrition?: NutritionEvent | null;
};

export type AnimalsListQuery = SchemaType<
  NonNullable<EndpointFor<"GET", "/animals">["request_schema"]["query"]>
>;
export type EventsListQuery = SchemaType<
  NonNullable<EndpointFor<"GET", "/events">["request_schema"]["query"]>
>;
export type AnimalEventsListQuery = SchemaType<
  NonNullable<EndpointFor<"GET", "/animals/:uid/events">["request_schema"]["query"]>
>;
export type CostsListQuery = SchemaType<
  NonNullable<EndpointFor<"GET", "/costs">["request_schema"]["query"]>
>;

export type AnimalCreate = SchemaType<
  EndpointFor<"POST", "/animals">["request_schema"]["body"]
>;
export type AnimalUpdate = SchemaType<
  EndpointFor<"PATCH", "/animals/:uid">["request_schema"]["body"]
>;
export type EventCreate = SchemaType<
  EndpointFor<"POST", "/events">["request_schema"]["body"]
>;
export type EventBulkCreate = SchemaType<
  EndpointFor<"POST", "/events/bulk">["request_schema"]["body"]
>;
export type CostCreate = SchemaType<
  EndpointFor<"POST", "/costs">["request_schema"]["body"]
>;
export type CostBulkCreate = SchemaType<
  EndpointFor<"POST", "/costs/bulk">["request_schema"]["body"]
>;
export type LocationCreate = SchemaType<
  EndpointFor<"POST", "/locations">["request_schema"]["body"]
>;
export type GroupCreate = SchemaType<
  EndpointFor<"POST", "/groups">["request_schema"]["body"]
>;
export type PartyCreate = SchemaType<
  EndpointFor<"POST", "/parties">["request_schema"]["body"]
>;
export type ProductCreate = SchemaType<
  EndpointFor<"POST", "/products">["request_schema"]["body"]
>;

export type Paginated<T> = {
  data: T[];
  nextCursor: string | null;
};

export type AnalyticsSummary = SchemaType<
  EndpointFor<"GET", "/dashboards/summary">["response_schema"]["200"]
>;
export type AnimalMetrics = SchemaType<
  EndpointFor<"GET", "/animals/:uid/metrics">["response_schema"]["200"]
>;

export type DimensionRecord = Location | HerdGroup | Party | Product;

export type FieldType<T extends string> = T extends "String"
  ? string
  : T extends "Int"
    ? number
    : T extends "Boolean"
      ? boolean
      : T extends "DateTime"
        ? string
        : T extends "BigInt"
          ? string
          : T extends `Decimal(${string})`
            ? string
            : T extends "Json"
              ? unknown
              : T extends "Role"
                ? Role
                : unknown;

type OptionalKeys<M extends ModelFields> = {
  [K in keyof M]: M[K] extends { optional: true } ? K : never;
}[keyof M];

type RequiredKeysOfModel<M extends ModelFields> = Exclude<keyof M, OptionalKeys<M>>;

export type ModelType<M extends ModelFields> = {
  [K in RequiredKeysOfModel<M>]: FieldType<M[K]["type"]>;
} & {
  [K in OptionalKeys<M>]?: FieldType<M[K]["type"]> | null;
};
