import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAdapters } from "../../adapters/AdapterContext";
import { useAppStore } from "../../state/store";
import { eventTypeValues } from "../../contract/types";
import type { EventCreate } from "../../contract/types";
import { isAdapterError } from "../../adapters/errors";

const emptyEvent: EventCreate = {
  uid: "",
  event_at: "",
  event_type: eventTypeValues[0]
};

export const EventsPage = () => {
  const { dataAdapter } = useAdapters();
  const { actions } = useAppStore.getState();
  const events = useAppStore((state) => state.events);
  const eventOrder = useAppStore((state) => state.eventOrder);
  const [eventInput, setEventInput] = useState<EventCreate>(emptyEvent);
  const [payloadText, setPayloadText] = useState("");
  const [idempotencyKey, setIdempotencyKey] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const eventsQuery = useQuery({
    queryKey: ["events"],
    queryFn: async () => dataAdapter.listEvents({ limit: 50 })
  });
  useEffect(() => {
    if (eventsQuery.data) actions.upsertEvents(eventsQuery.data.data);
  }, [eventsQuery.data, actions]);

  const createMutation = useMutation({
    mutationFn: async (payload: EventCreate) =>
      dataAdapter.createEvent(payload, { idempotencyKey: idempotencyKey || undefined }),
    onSuccess: (event) => {
      actions.upsertEvents([event]);
      setEventInput(emptyEvent);
      setPayloadText("");
      setIdempotencyKey("");
      setErrorMessage(null);
    },
    onError: (error) => {
      setErrorMessage(isAdapterError(error) ? error.message : "Failed to create event");
    }
  });

  const eventList = useMemo(
    () => eventOrder.map((id) => events[id]).filter(Boolean),
    [eventOrder, events]
  );

  return (
    <section className="page">
      <div className="page-header">
        <h2>Events</h2>
        <div className="page-meta">{eventList.length} events</div>
      </div>

      {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}

      <div className="panel">
        <h3>Create Event</h3>
        <div className="form-grid">
          <label>
            UID
            <input
              value={eventInput.uid ?? ""}
              onChange={(event) => setEventInput({ ...eventInput, uid: event.target.value })}
            />
          </label>
          <label>
            Event At (ISO)
            <input
              value={eventInput.event_at ?? ""}
              onChange={(event) => setEventInput({ ...eventInput, event_at: event.target.value })}
              placeholder="2026-02-04T11:59:00.000Z"
            />
          </label>
          <label>
            Event Type
            <select
              value={eventInput.event_type}
              onChange={(event) =>
                setEventInput({
                  ...eventInput,
                  event_type: event.target.value as EventCreate["event_type"]
                })
              }
            >
              {eventTypeValues.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            Event Subtype
            <input
              value={eventInput.event_subtype ?? ""}
              onChange={(event) =>
                setEventInput({ ...eventInput, event_subtype: event.target.value })
              }
            />
          </label>
          <label>
            Source Ref
            <input
              value={eventInput.source_ref ?? ""}
              onChange={(event) => setEventInput({ ...eventInput, source_ref: event.target.value })}
            />
          </label>
          <label>
            Batch ID
            <input
              value={eventInput.batch_id ?? ""}
              onChange={(event) => setEventInput({ ...eventInput, batch_id: event.target.value })}
            />
          </label>
          <label>
            Confidence
            <input
              type="number"
              value={eventInput.confidence ?? ""}
              onChange={(event) =>
                setEventInput({
                  ...eventInput,
                  confidence: event.target.value ? Number(event.target.value) : undefined
                })
              }
            />
          </label>
          <label>
            Notes
            <input
              value={eventInput.notes ?? ""}
              onChange={(event) => setEventInput({ ...eventInput, notes: event.target.value })}
            />
          </label>
          <label>
            Idempotency Key (header)
            <input value={idempotencyKey} onChange={(event) => setIdempotencyKey(event.target.value)} />
          </label>
        </div>

        <div className="form-section">
          <div className="section-title">Dimension Codes</div>
          <div className="form-grid">
            <label>
              Location From Code
              <input
                value={eventInput.location_from_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, location_from_code: event.target.value })
                }
              />
            </label>
            <label>
              Location To Code
              <input
                value={eventInput.location_to_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, location_to_code: event.target.value })
                }
              />
            </label>
            <label>
              Group Code
              <input
                value={eventInput.group_code ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, group_code: event.target.value })}
              />
            </label>
            <label>
              Party Code
              <input
                value={eventInput.party_code ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, party_code: event.target.value })}
              />
            </label>
            <label>
              Product Code
              <input
                value={eventInput.product_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, product_code: event.target.value })
                }
              />
            </label>
          </div>
        </div>

        <div className="form-section">
          <div className="section-title">Subtype Fields</div>
          <div className="form-grid">
            <label>
              Weight (kg)
              <input
                type="number"
                value={eventInput.weight_kg ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    weight_kg: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Method
              <input
                value={eventInput.method ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, method: event.target.value })}
              />
            </label>
            <label>
              Shrink %
              <input
                type="number"
                value={eventInput.shrink_pct ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    shrink_pct: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Reason
              <input
                value={eventInput.reason ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, reason: event.target.value })}
              />
            </label>
            <label>
              Distance (km)
              <input
                type="number"
                value={eventInput.distance_km ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    distance_km: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Transport Party Code
              <input
                value={eventInput.transport_party_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, transport_party_code: event.target.value })
                }
              />
            </label>
            <label>
              Repro Action
              <input
                value={eventInput.repro_action ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, repro_action: event.target.value })
                }
              />
            </label>
            <label>
              Sire UID
              <input
                value={eventInput.sire_uid ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, sire_uid: event.target.value })}
              />
            </label>
            <label>
              Dam UID
              <input
                value={eventInput.dam_uid ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, dam_uid: event.target.value })}
              />
            </label>
            <label>
              Result
              <input
                value={eventInput.result ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, result: event.target.value })}
              />
            </label>
            <label>
              Calf UID
              <input
                value={eventInput.calf_uid ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, calf_uid: event.target.value })}
              />
            </label>
            <label>
              Gestation Days
              <input
                type="number"
                value={eventInput.gestation_days ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    gestation_days: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Action
              <input
                value={eventInput.action ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, action: event.target.value })}
              />
            </label>
            <label>
              Diagnosis
              <input
                value={eventInput.diagnosis ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, diagnosis: event.target.value })}
              />
            </label>
            <label>
              Dose
              <input
                type="number"
                value={eventInput.dose ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    dose: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Dose Unit
              <input
                value={eventInput.dose_unit ?? ""}
                onChange={(event) => setEventInput({ ...eventInput, dose_unit: event.target.value })}
              />
            </label>
            <label>
              Withdrawal Days
              <input
                type="number"
                value={eventInput.withdrawal_days ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    withdrawal_days: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Ration Code
              <input
                value={eventInput.ration_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, ration_code: event.target.value })
                }
              />
            </label>
            <label>
              Intake (kg/day)
              <input
                type="number"
                value={eventInput.intake_kg_day ?? ""}
                onChange={(event) =>
                  setEventInput({
                    ...eventInput,
                    intake_kg_day: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Supplement Code
              <input
                value={eventInput.supplement_code ?? ""}
                onChange={(event) =>
                  setEventInput({ ...eventInput, supplement_code: event.target.value })
                }
              />
            </label>
          </div>
        </div>

        <label className="payload-block">
          Payload (JSON)
          <textarea
            value={payloadText}
            onChange={(event) => setPayloadText(event.target.value)}
            placeholder='{"key":"value"}'
          />
        </label>

        <button
          type="button"
          className="primary"
          onClick={() => {
            let payloadValue: EventCreate["payload"] = undefined;
            if (payloadText.trim()) {
              try {
                payloadValue = JSON.parse(payloadText);
              } catch {
                setErrorMessage("Payload must be valid JSON");
                return;
              }
            }
            setErrorMessage(null);
            createMutation.mutate({ ...eventInput, payload: payloadValue });
          }}
          disabled={!eventInput.uid || !eventInput.event_at || createMutation.isPending}
        >
          Create Event
        </button>
        <div className="helper-text">
          idempotency-key overrides source_ref for /events and /events/bulk.
        </div>
      </div>

      <div className="panel">
        <h3>Event Log</h3>
        <div className="card-grid">
          {eventList.map((event) => (
            <div key={event.event_id} className="card">
              <div className="card-title">
                {event.event_type} · {event.uid}
              </div>
              <div className="card-subtitle">{event.event_at}</div>
              <div className="card-body">
                <div>Event ID: {event.event_id}</div>
                <div>Subtype: {event.event_subtype ?? "-"}</div>
                <div>Weight: {event.weight?.weight_kg ?? "-"}</div>
                <div>From: {event.location_from_id ?? "-"}</div>
                <div>To: {event.location_to_id ?? "-"}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
};
