import { useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAdapters } from "../../adapters/AdapterContext";
import { useAppStore } from "../../state/store";
import { selectDerivedMetrics } from "../../state/selectors";
import type { AnimalCreate, AnimalUpdate } from "../../contract/types";
import { isAdapterError } from "../../adapters/errors";

const emptyCreate: AnimalCreate = { uid: "" };

export const AnimalsPage = () => {
  const { dataAdapter } = useAdapters();
  const { actions } = useAppStore.getState();
  const storeSnapshot = useAppStore((state) => state);
  const animals = storeSnapshot.animals;
  const [createInput, setCreateInput] = useState<AnimalCreate>(emptyCreate);
  const [updateInput, setUpdateInput] = useState<AnimalUpdate>({});
  const [updateUid, setUpdateUid] = useState("");
  const [ifMatchVersion, setIfMatchVersion] = useState("");
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const { isLoading } = useQuery({
    queryKey: ["animals"],
    queryFn: async () => dataAdapter.listAnimals({ limit: 50 }),
    onSuccess: (result) => actions.upsertAnimals(result.data)
  });

  const createMutation = useMutation({
    mutationFn: async (payload: AnimalCreate) => dataAdapter.createAnimal(payload),
    onSuccess: (animal) => {
      actions.upsertAnimals([animal]);
      setCreateInput(emptyCreate);
      setErrorMessage(null);
    },
    onError: (error) => {
      setErrorMessage(isAdapterError(error) ? error.message : "Failed to create animal");
    }
  });

  const updateMutation = useMutation({
    mutationFn: async ({ uid, input }: { uid: string; input: AnimalUpdate }) =>
      dataAdapter.updateAnimal(uid, input, { ifMatchVersion: Number(ifMatchVersion) }),
    onSuccess: (animal) => {
      actions.upsertAnimals([animal]);
      setUpdateInput({});
      setUpdateUid("");
      setIfMatchVersion("");
      setErrorMessage(null);
    },
    onError: (error) => {
      setErrorMessage(isAdapterError(error) ? error.message : "Failed to update animal");
    }
  });

  const animalList = useMemo(() => Object.values(animals), [animals]);

  return (
    <section className="page">
      <div className="page-header">
        <h2>Animals</h2>
        <div className="page-meta">{isLoading ? "Loading..." : `${animalList.length} animals`}</div>
      </div>

      {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}

      <div className="panel-grid">
        <div className="panel">
          <h3>Create Animal</h3>
          <div className="form-grid">
            <label>
              UID (required)
              <input
                value={createInput.uid ?? ""}
                onChange={(event) => setCreateInput({ ...createInput, uid: event.target.value })}
                placeholder="A-001"
              />
            </label>
            <label>
              EID
              <input
                value={createInput.eid ?? ""}
                onChange={(event) => setCreateInput({ ...createInput, eid: event.target.value })}
              />
            </label>
            <label>
              Race
              <input
                value={createInput.race ?? ""}
                onChange={(event) => setCreateInput({ ...createInput, race: event.target.value })}
              />
            </label>
            <label>
              Sex
              <input
                value={createInput.sex ?? ""}
                onChange={(event) => setCreateInput({ ...createInput, sex: event.target.value })}
              />
            </label>
            <label>
              Birth Year
              <input
                type="number"
                value={createInput.birth_year ?? ""}
                onChange={(event) =>
                  setCreateInput({
                    ...createInput,
                    birth_year: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Birth Month
              <input
                type="number"
                value={createInput.birth_month ?? ""}
                onChange={(event) =>
                  setCreateInput({
                    ...createInput,
                    birth_month: event.target.value ? Number(event.target.value) : undefined
                  })
                }
              />
            </label>
            <label>
              Brand Mark
              <input
                value={createInput.brand_mark ?? ""}
                onChange={(event) =>
                  setCreateInput({ ...createInput, brand_mark: event.target.value })
                }
              />
            </label>
            <label className="checkbox">
              <input
                type="checkbox"
                checked={createInput.alert ?? false}
                onChange={(event) => setCreateInput({ ...createInput, alert: event.target.checked })}
              />
              Alert
            </label>
          </div>
          <button
            type="button"
            className="primary"
            onClick={() => createMutation.mutate(createInput)}
            disabled={!createInput.uid || createMutation.isPending}
          >
            Create
          </button>
        </div>

        <div className="panel">
          <h3>Update Animal</h3>
          <div className="form-grid">
            <label>
              UID
              <input
                value={updateUid}
                onChange={(event) => setUpdateUid(event.target.value)}
              />
            </label>
            <label>
              if-match-version
              <input
                type="number"
                value={ifMatchVersion}
                onChange={(event) => setIfMatchVersion(event.target.value)}
              />
            </label>
            <label>
              Warning
              <input
                value={updateInput.warning ?? ""}
                onChange={(event) => setUpdateInput({ ...updateInput, warning: event.target.value })}
              />
            </label>
            <label>
              Notes
              <input
                value={updateInput.notes ?? ""}
                onChange={(event) => setUpdateInput({ ...updateInput, notes: event.target.value })}
              />
            </label>
            <label className="checkbox">
              <input
                type="checkbox"
                checked={updateInput.alert ?? false}
                onChange={(event) => setUpdateInput({ ...updateInput, alert: event.target.checked })}
              />
              Alert
            </label>
          </div>
          <button
            type="button"
            className="primary"
            onClick={() => updateMutation.mutate({ uid: updateUid, input: updateInput })}
            disabled={!updateUid || !ifMatchVersion || updateMutation.isPending}
          >
            Update
          </button>
          <div className="helper-text">
            PATCH requires the exact if-match-version header; mismatch returns 409.
          </div>
        </div>
      </div>

      <div className="panel">
        <h3>Animal List</h3>
        <div className="card-grid">
          {animalList.map((animal) => {
            const metrics = selectDerivedMetrics(storeSnapshot, animal.uid);
            return (
              <div key={animal.uid} className="card">
                <div className="card-title">{animal.uid}</div>
                <div className="card-subtitle">{animal.race ?? "Unspecified race"}</div>
                <div className="card-body">
                  <div>Sex: {animal.sex ?? "-"}</div>
                  <div>Brand: {animal.brand_mark ?? "-"}</div>
                  <div>Last weight: {metrics.lastWeight ?? "-"}</div>
                  <div>ADG: {metrics.adg ?? "-"}</div>
                  <div>Last location: {metrics.lastLocationCode ?? "-"}</div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </section>
  );
};
