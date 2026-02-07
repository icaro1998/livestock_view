import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useAdapters } from "../../adapters/AdapterContext";
import { useAppStore } from "../../state/store";
import { dimensionFieldSupportByTable } from "../../contract/types";
import type {
  DimensionTable,
  GroupCreate,
  LocationCreate,
  PartyCreate,
  ProductCreate
} from "../../contract/types";
import { isAdapterError } from "../../adapters/errors";

const fieldLabels: Record<string, string> = {
  code: "Code",
  name: "Name",
  type: "Type",
  category: "Category",
  unit: "Unit",
  meta: "Meta (JSON)"
};

type DimensionFormState = {
  code?: string;
  name?: string;
  type?: string;
  category?: string;
  unit?: string;
  meta?: string;
};

const emptyForm: DimensionFormState = {};

type SectionConfig = {
  key: DimensionTable;
  label: string;
  storeKey: "locations" | "groups" | "parties" | "products";
  list: () => Promise<any[]>;
  create: (payload: any) => Promise<any>;
};

const DimensionSection = ({
  section,
  formState,
  onFormChange,
  records,
  onUpsert,
  onError
}: {
  section: SectionConfig;
  formState: DimensionFormState;
  onFormChange: (next: DimensionFormState) => void;
  records: any[];
  onUpsert: (records: any[]) => void;
  onError: (message: string) => void;
}) => {
  const allowedFields = dimensionFieldSupportByTable[section.key];

  const dimensionQuery = useQuery({
    queryKey: ["dimensions", section.key],
    queryFn: section.list
  });
  useEffect(() => {
    if (dimensionQuery.data) onUpsert(dimensionQuery.data);
  }, [dimensionQuery.data, onUpsert]);

  const mutation = useMutation({
    mutationFn: async () => {
      if (!formState.code) throw new Error("Code is required");
      const payload: any = { code: formState.code };
      allowedFields.forEach((field) => {
        if (field === "meta") {
          if (formState.meta) {
            payload.meta = JSON.parse(formState.meta);
          }
          return;
        }
        if (field !== "code" && (formState as any)[field]) {
          payload[field] = (formState as any)[field];
        }
      });
      return section.create(payload);
    },
    onSuccess: (record) => {
      onUpsert([record]);
      onFormChange(emptyForm);
      onError("");
    },
    onError: (error) => {
      const message = isAdapterError(error)
        ? error.message
        : error instanceof Error
          ? error.message
          : "Failed to create dimension";
      onError(message);
    }
  });

  return (
    <div className="panel">
      <h3>{section.label}</h3>
      <div className="form-grid">
        {allowedFields.map((field) => (
          <label key={field}>
            {fieldLabels[field] ?? field}
            {field === "meta" ? (
              <textarea
                value={formState.meta ?? ""}
                onChange={(event) => onFormChange({ ...formState, meta: event.target.value })}
                placeholder='{"key":"value"}'
              />
            ) : (
              <input
                value={(formState as any)[field] ?? ""}
                onChange={(event) => onFormChange({ ...formState, [field]: event.target.value })}
              />
            )}
          </label>
        ))}
      </div>
      <button
        type="button"
        className="primary"
        onClick={() => mutation.mutate()}
        disabled={!formState.code || mutation.isPending}
      >
        Create {section.label.slice(0, -1)}
      </button>

      <div className="card-grid">
        {records.map((record: any) => (
          <div key={record.code} className="card">
            <div className="card-title">{record.code}</div>
            <div className="card-subtitle">{record.name ?? "Unnamed"}</div>
            <div className="card-body">
              <div>Type: {record.type ?? record.category ?? "-"}</div>
              <div>Unit: {record.unit ?? "-"}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export const DimensionsPage = () => {
  const { dataAdapter } = useAdapters();
  const { actions } = useAppStore.getState();
  const dimensions = useAppStore((state) => state.dimensions);

  const [forms, setForms] = useState<Record<string, DimensionFormState>>({});
  const [errorMessage, setErrorMessage] = useState<string | null>(null);

  const sections: SectionConfig[] = [
    {
      key: "location",
      label: "Locations",
      storeKey: "locations",
      list: () => dataAdapter.listLocations(),
      create: (payload: LocationCreate) => dataAdapter.createLocation(payload)
    },
    {
      key: "herdGroup",
      label: "Groups",
      storeKey: "groups",
      list: () => dataAdapter.listGroups(),
      create: (payload: GroupCreate) => dataAdapter.createGroup(payload)
    },
    {
      key: "party",
      label: "Parties",
      storeKey: "parties",
      list: () => dataAdapter.listParties(),
      create: (payload: PartyCreate) => dataAdapter.createParty(payload)
    },
    {
      key: "product",
      label: "Products",
      storeKey: "products",
      list: () => dataAdapter.listProducts(),
      create: (payload: ProductCreate) => dataAdapter.createProduct(payload)
    }
  ];

  return (
    <section className="page">
      <div className="page-header">
        <h2>Dimensions</h2>
        <div className="page-meta">Auto-create enabled for event/cost codes</div>
      </div>

      {errorMessage ? <div className="error-banner">{errorMessage}</div> : null}

      <div className="panel-grid">
        {sections.map((section) => (
          <DimensionSection
            key={section.key}
            section={section}
            formState={forms[section.key] ?? emptyForm}
            onFormChange={(next) =>
              setForms((prev) => ({
                ...prev,
                [section.key]: next
              }))
            }
            records={Object.values(dimensions[section.storeKey].byCode)}
            onUpsert={(records) => actions.upsertDimensions(section.storeKey, records)}
            onError={(message) => setErrorMessage(message)}
          />
        ))}
      </div>
    </section>
  );
};
