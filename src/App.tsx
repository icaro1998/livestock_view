import { useState } from "react";
import { Layout } from "./ui/Layout";
import type { PageKey } from "./ui/Layout";
import { AnimalsPage } from "./ui/pages/AnimalsPage";
import { EventsPage } from "./ui/pages/EventsPage";
import { CostsPage } from "./ui/pages/CostsPage";
import { DimensionsPage } from "./ui/pages/DimensionsPage";
import { AnalyticsPage } from "./ui/pages/AnalyticsPage";

export const App = () => {
  const [active, setActive] = useState<PageKey>("animals");

  return (
    <Layout active={active} onSelect={setActive}>
      {active === "animals" && <AnimalsPage />}
      {active === "events" && <EventsPage />}
      {active === "costs" && <CostsPage />}
      {active === "dimensions" && <DimensionsPage />}
      {active === "analytics" && <AnalyticsPage />}
    </Layout>
  );
};
