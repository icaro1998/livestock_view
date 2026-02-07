import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAdapters } from "../../adapters/AdapterContext";
import { useAppStore } from "../../state/store";
import { costCategoryValues, costScopeValues } from "../../contract/types";
import type { CostCreate } from "../../contract/types";
import { isAdapterError } from "../../adapters/errors";

const emptyCost: CostCreate = {
  cost_at: "",
  scope: costScopeValues[0],
  category: costCategoryValues[0],
  amount: 0
};

export const CostsPage = () => {
  const { dataAdapter } = useAdapters();
  const { actions } = useAppStore.getState();
  const costs = useAppStore((state) => state.costs);
  const costOrder = useAppStore((state) => state.costOrder);
  const [costInput, setCostInput] = useState<CostCreate>(emptyCost);
  const [payloadText, setPayloadText] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const costsQuery = useQuery({
    queryKey: ["costs"],
    queryFn: async () => dataAdapter.listCosts({ limit: 50 })
  });
  useEffect(() => {
    if (costsQuery.data) actions.upsertCosts(costsQuery.data.data);
  }, [costsQuery.data, actions]);

  const createMutation = useMutation({
    mutationFn: async (payload: CostCreate) => dataAdapter.createCost(payload),
    onSuccess: (cost) => {
      actions.upsertCosts([cost]);
      setCostInput(emptyCost);
      setPayloadText("");
      setErrorMessage(null);
    },
    onError: (error) => {
      setErrorMessage(isAdapterError(error) ? error.message : "Failed to create cost");
    }
  });

  const costList = useMemo(
    () => costOrder.map((id) => costs[id]).filter(Boolean),
    [costOrder, costs]
  );

  return (
    <section className="page">
      <div className="page-header">
        <h2>Costs</h2>
        <div className="page-meta">{costList.length} costs</div>
      </div>

      {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}

      <div className="panel">
        <h3>Create Cost</h3>
        <div className="form-grid">
          <label>
            Cost At (ISO)
            <input
              value={costInput.cost_at ?? ""}
              onChange={(event) => setCostInput({ ...costInput, cost_at: event.target.value })}
              placeholder="2026-02-04T00:00:00.000Z"
            />
          </label>
          <label>
            Scope
            <select
              value={costInput.scope}
              onChange={(event) =>
                setCostInput({
                  ...costInput,
                  scope: event.target.value as CostCreate["scope"]
                })
              }
            >
              {costScopeValues.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            Category
            <select
              value={costInput.category}
              onChange={(event) =>
                setCostInput({
                  ...costInput,
                  category: event.target.value as CostCreate["category"]
                })
              }
            >
              {costCategoryValues.map((value) => (
                <option key={value} value={value}>
                  {value}
                </option>
              ))}
            </select>
          </label>
          <label>
            Amount
            <input
              type="number"
              value={costInput.amount ?? ""}
              onChange={(event) =>
                setCostInput({
                  ...costInput,
                  amount: event.target.value ? Number(event.target.value) : 0
                })
              }
            />
          </label>
          <label>
            Currency
            <input
              value={costInput.currency ?? ""}
              onChange={(event) => setCostInput({ ...costInput, currency: event.target.value })}
              placeholder="BOB"
            />
          </label>
          <label>
            UID
            <input
              value={costInput.uid ?? ""}
              onChange={(event) => setCostInput({ ...costInput, uid: event.target.value })}
            />
          </label>
          <label>
            Group Code
            <input
              value={costInput.group_code ?? ""}
              onChange={(event) => setCostInput({ ...costInput, group_code: event.target.value })}
            />
          </label>
          <label>
            Location Code
            <input
              value={costInput.location_code ?? ""}
              onChange={(event) =>
                setCostInput({ ...costInput, location_code: event.target.value })
              }
            />
          </label>
          <label>
            Product Code
            <input
              value={costInput.product_code ?? ""}
              onChange={(event) =>
                setCostInput({ ...costInput, product_code: event.target.value })
              }
            />
          </label>
          <label>
            Party Code
            <input
              value={costInput.party_code ?? ""}
              onChange={(event) => setCostInput({ ...costInput, party_code: event.target.value })}
            />
          </label>
          <label>
            Quantity
            <input
              type="number"
              value={costInput.quantity ?? ""}
              onChange={(event) =>
                setCostInput({
                  ...costInput,
                  quantity: event.target.value ? Number(event.target.value) : undefined
                })
              }
            />
          </label>
          <label>
            Unit
            <input
              value={costInput.unit ?? ""}
              onChange={(event) => setCostInput({ ...costInput, unit: event.target.value })}
            />
          </label>
          <label>
            Source Ref
            <input
              value={costInput.source_ref ?? ""}
              onChange={(event) => setCostInput({ ...costInput, source_ref: event.target.value })}
            />
          </label>
          <label>
            Batch ID
            <input
              value={costInput.batch_id ?? ""}
              onChange={(event) => setCostInput({ ...costInput, batch_id: event.target.value })}
            />
          </label>
          <label>
            Notes
            <input
              value={costInput.notes ?? ""}
              onChange={(event) => setCostInput({ ...costInput, notes: event.target.value })}
            />
          </label>
        </div>

        <label className="payload-block">
          Payload (JSON) - not persisted by backend
          <textarea
            value={payloadText}
            onChange={(event) => setPayloadText(event.target.value)}
            placeholder='{"detail":"not persisted"}'
          />
        </label>

        <button
          type="button"
          className="primary"
          onClick={() => {
            let payloadValue: CostCreate["payload"] = undefined;
            if (payloadText.trim()) {
              try {
                payloadValue = JSON.parse(payloadText);
              } catch {
                setErrorMessage("Payload must be valid JSON");
                return;
              }
            }
            setErrorMessage(null);
            createMutation.mutate({ ...costInput, payload: payloadValue });
          }}
          disabled={!costInput.cost_at || costInput.amount === undefined || createMutation.isPending}
        >
          Create Cost
        </button>
        <div className="helper-text">
          Cost payload is accepted but not persisted by the backend.
        </div>
      </div>

      <div className="panel">
        <h3>Cost Log</h3>
        <div className="card-grid">
          {costList.map((cost) => (
            <div key={cost.cost_id} className="card">
              <div className="card-title">
                {cost.category} · {cost.scope}
              </div>
              <div className="card-subtitle">{cost.cost_at}</div>
              <div className="card-body">
                <div>Cost ID: {cost.cost_id}</div>
                <div>Amount: {cost.amount} {cost.currency}</div>
                <div>UID: {cost.uid ?? "-"}</div>
                <div>Location ID: {cost.location_id ?? "-"}</div>
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
};
