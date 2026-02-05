import type { AnimalEventWithSubtypes } from "../contract/types";
import type { StoreState } from "./store";

const parseNumeric = (value: string | null | undefined) => {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
};

const sortByEventAtDesc = (events: AnimalEventWithSubtypes[]) =>
  [...events].sort((a, b) => Date.parse(b.event_at) - Date.parse(a.event_at));

export const selectAnimalEvents = (state: StoreState, uid: string) =>
  Object.values(state.events).filter((event) => event.uid === uid);

export const selectDerivedMetrics = (state: StoreState, uid: string) => {
  const events = selectAnimalEvents(state, uid);
  const sorted = sortByEventAtDesc(events);
  const weightEvents = sorted.filter((event) => event.event_type === "weight");
  const movementEvents = sorted.filter((event) => event.event_type === "movement");

  const lastWeightEvent = weightEvents[0];
  const prevWeightEvent = weightEvents[1];

  const lastWeight = parseNumeric(lastWeightEvent?.weight?.weight_kg ?? null);
  const prevWeight = parseNumeric(prevWeightEvent?.weight?.weight_kg ?? null);

  let adg: number | null = null;
  if (lastWeight !== null && prevWeight !== null && lastWeightEvent && prevWeightEvent) {
    const days =
      (Date.parse(lastWeightEvent.event_at) - Date.parse(prevWeightEvent.event_at)) /
      (1000 * 60 * 60 * 24);
    if (days > 0) {
      adg = Number(((lastWeight - prevWeight) / days).toFixed(2));
    }
  }

  const lastMovement = movementEvents[0];
  const locationId = lastMovement?.location_to_id ?? lastMovement?.location_from_id ?? null;
  const location = locationId ? state.dimensions.locations.byId[locationId] : undefined;

  return {
    lastWeight,
    lastWeightAt: lastWeightEvent?.event_at ?? null,
    prevWeight,
    prevWeightAt: prevWeightEvent?.event_at ?? null,
    adg,
    lastLocationCode: location?.code ?? null
  };
};
