import { create } from "zustand";
import type {
  Animal,
  AnimalEventWithSubtypes,
  AnimalMetrics,
  AnalyticsSummary,
  CostEvent,
  HerdGroup,
  Location,
  Party,
  Product,
  Role
} from "../contract/types";

export type DimensionBucket<T> = {
  byCode: Record<string, T>;
  byId: Record<string, T>;
};

export type DimensionsState = {
  locations: DimensionBucket<Location>;
  groups: DimensionBucket<HerdGroup>;
  parties: DimensionBucket<Party>;
  products: DimensionBucket<Product>;
};

export type StoreState = {
  role: Role;
  animals: Record<string, Animal>;
  events: Record<string, AnimalEventWithSubtypes>;
  eventOrder: string[];
  costs: Record<string, CostEvent>;
  costOrder: string[];
  dimensions: DimensionsState;
  analyticsSummary?: AnalyticsSummary;
  animalMetrics: Record<string, AnimalMetrics>;
  actions: {
    setRole: (role: Role) => void;
    upsertAnimals: (animals: Animal[]) => void;
    upsertEvents: (events: AnimalEventWithSubtypes[]) => void;
    upsertCosts: (costs: CostEvent[]) => void;
    upsertDimensions: (table: keyof DimensionsState, records: any[]) => void;
    setAnalyticsSummary: (summary: AnalyticsSummary) => void;
    setAnimalMetrics: (uid: string, metrics: AnimalMetrics) => void;
  };
};

const emptyDimensions = (): DimensionsState => ({
  locations: { byCode: {}, byId: {} },
  groups: { byCode: {}, byId: {} },
  parties: { byCode: {}, byId: {} },
  products: { byCode: {}, byId: {} }
});

const sortByEventAt = (events: Record<string, AnimalEventWithSubtypes>) =>
  Object.values(events)
    .sort((a, b) => Date.parse(b.event_at) - Date.parse(a.event_at))
    .map((event) => event.event_id);

const sortByCostAt = (costs: Record<string, CostEvent>) =>
  Object.values(costs)
    .sort((a, b) => Date.parse(b.cost_at) - Date.parse(a.cost_at))
    .map((cost) => cost.cost_id);

export const useAppStore = create<StoreState>((set) => ({
  role: "viewer",
  animals: {},
  events: {},
  eventOrder: [],
  costs: {},
  costOrder: [],
  dimensions: emptyDimensions(),
  analyticsSummary: undefined,
  animalMetrics: {},
  actions: {
    setRole: (role) => set({ role }),
    upsertAnimals: (animals) => {
      set((state) => {
        const next = { ...state.animals };
        animals.forEach((animal) => {
          next[animal.uid] = animal;
        });
        return { animals: next };
      });
    },
    upsertEvents: (events) => {
      set((state) => {
        const next = { ...state.events };
        events.forEach((event) => {
          next[event.event_id] = event;
        });
        return { events: next, eventOrder: sortByEventAt(next) };
      });
    },
    upsertCosts: (costs) => {
      set((state) => {
        const next = { ...state.costs };
        costs.forEach((cost) => {
          next[cost.cost_id] = cost;
        });
        return { costs: next, costOrder: sortByCostAt(next) };
      });
    },
    upsertDimensions: (table, records) => {
      set((state) => {
        const next = { ...state.dimensions };
        const bucket = { ...next[table] } as DimensionBucket<any>;
        const nextByCode = { ...bucket.byCode };
        const nextById = { ...bucket.byId };
        records.forEach((record) => {
          if (record?.code) nextByCode[record.code] = record;
          if (record?.id) nextById[record.id] = record;
        });
        next[table] = { byCode: nextByCode, byId: nextById } as any;
        return { dimensions: next };
      });
    },
    setAnalyticsSummary: (summary) => set({ analyticsSummary: summary }),
    setAnimalMetrics: (uid, metrics) =>
      set((state) => ({
        animalMetrics: { ...state.animalMetrics, [uid]: metrics }
      }))
  }
}));
