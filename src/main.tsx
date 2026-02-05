import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClientProvider } from "@tanstack/react-query";
import { queryClient } from "./query/client";
import { AdaptersProvider } from "./adapters/AdapterContext";
import { App } from "./App";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <AdaptersProvider>
        <App />
      </AdaptersProvider>
    </QueryClientProvider>
  </React.StrictMode>
);
