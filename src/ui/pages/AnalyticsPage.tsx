import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useAdapters } from "../../adapters/AdapterContext";
import { useAppStore } from "../../state/store";
import { selectDerivedMetrics } from "../../state/selectors";

export const AnalyticsPage = () => {
  const { dataAdapter } = useAdapters();
  const { actions } = useAppStore.getState();
  const storeSnapshot = useAppStore((state) => state);
  const summary = storeSnapshot.analyticsSummary;
  const [uid, setUid] = useState("");

  const summaryQuery = useQuery({
    queryKey: ["analytics", "summary"],
    queryFn: async () => dataAdapter.getAnalyticsSummary()
  });
  useEffect(() => {
    if (summaryQuery.data) actions.setAnalyticsSummary(summaryQuery.data);
  }, [summaryQuery.data, actions]);

  const metricsQuery = useQuery({
    queryKey: ["analytics", "metrics", uid],
    queryFn: async () => dataAdapter.getAnimalMetrics(uid),
    enabled: Boolean(uid)
  });
  useEffect(() => {
    if (uid && metricsQuery.data) actions.setAnimalMetrics(uid, metricsQuery.data);
  }, [uid, metricsQuery.data, actions]);

  const derived = uid ? selectDerivedMetrics(storeSnapshot, uid) : null;

  return (
    <section className="page">
      <div className="page-header">
        <h2>Analytics</h2>
        <div className="page-meta">Summary and animal metrics</div>
      </div>

      <div className="panel-grid">
        <div className="panel">
          <h3>Summary</h3>
          <pre className="code-block">{JSON.stringify(summary ?? {}, null, 2)}</pre>
        </div>

        <div className="panel">
          <h3>Animal Metrics</h3>
          <label>
            Animal UID
            <input value={uid} onChange={(event) => setUid(event.target.value)} />
          </label>
          <pre className="code-block">
            {uid ? JSON.stringify(metricsQuery.data ?? {}, null, 2) : "Enter a UID"}
          </pre>
          <div className="helper-text">Derived from event log when available.</div>
          {derived ? (
            <div className="derived-metrics">
              <div>Last weight: {derived.lastWeight ?? "-"}</div>
              <div>ADG: {derived.adg ?? "-"}</div>
              <div>Last location: {derived.lastLocationCode ?? "-"}</div>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
};
