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
  ProductCreate
} from "../contract/types";

export interface IDataAdapter {
  listAnimals(params: AnimalsListQuery): Promise<Paginated<Animal>>;
  getAnimal(uid: string): Promise<Animal>;
  createAnimal(input: AnimalCreate): Promise<Animal>;
  updateAnimal(
    uid: string,
    input: AnimalUpdate,
    options: { ifMatchVersion: number }
  ): Promise<Animal>;
  listAnimalEvents(
    uid: string,
    params?: AnimalEventsListQuery
  ): Promise<Paginated<AnimalEventWithSubtypes>>;

  listEvents(params?: EventsListQuery): Promise<Paginated<AnimalEventWithSubtypes>>;
  createEvent(
    input: EventCreate,
    options?: { idempotencyKey?: string }
  ): Promise<AnimalEventWithSubtypes>;
  createEventsBulk(
    input: EventBulkCreate,
    options?: { idempotencyKey?: string }
  ): Promise<AnimalEventWithSubtypes[]>;

  listCosts(params?: CostsListQuery): Promise<Paginated<CostEvent>>;
  createCost(input: CostCreate): Promise<CostEvent>;
  createCostsBulk(input: CostBulkCreate): Promise<CostEvent[]>;

  listLocations(): Promise<Location[]>;
  createLocation(input: LocationCreate): Promise<Location>;
  listGroups(): Promise<HerdGroup[]>;
  createGroup(input: GroupCreate): Promise<HerdGroup>;
  listParties(): Promise<Party[]>;
  createParty(input: PartyCreate): Promise<Party>;
  listProducts(): Promise<Product[]>;
  createProduct(input: ProductCreate): Promise<Product>;

  getAnalyticsSummary(): Promise<AnalyticsSummary>;
  getAnimalMetrics(uid: string): Promise<AnimalMetrics>;
}
