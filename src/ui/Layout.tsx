import type { ReactNode } from "react";
import { contract } from "../contract/contract.generated";
import type { Role } from "../contract/types";
import { useAppStore } from "../state/store";

export type PageKey = "animals" | "events" | "costs" | "dimensions" | "analytics";

const pages: { key: PageKey; label: string }[] = [
  { key: "animals", label: "Animals" },
  { key: "events", label: "Events" },
  { key: "costs", label: "Costs" },
  { key: "dimensions", label: "Dimensions" },
  { key: "analytics", label: "Analytics" }
];

export const Layout = ({
  active,
  onSelect,
  children
}: {
  active: PageKey;
  onSelect: (key: PageKey) => void;
  children: ReactNode;
}) => {
  const role = useAppStore((state) => state.role);
  const setRole = useAppStore((state) => state.actions.setRole);

  return (
    <div className="app-shell">
      <header className="app-header">
        <div>
          <div className="app-title">Livestock View</div>
          <div className="app-subtitle">Contract-driven UI shell</div>
        </div>
        <div className="role-switch">
          <label htmlFor="role-select">Role</label>
          <select
            id="role-select"
            value={role}
            onChange={(event) => setRole(event.target.value as Role)}
          >
            {contract.roles_acl.roles.map((roleOption) => (
              <option key={roleOption} value={roleOption}>
                {roleOption}
              </option>
            ))}
          </select>
        </div>
      </header>

      <nav className="app-nav">
        {pages.map((page) => (
          <button
            key={page.key}
            className={page.key === active ? "nav-button active" : "nav-button"}
            onClick={() => onSelect(page.key)}
            type="button"
          >
            {page.label}
          </button>
        ))}
      </nav>

      <section className="contract-warnings">
        <div className="warnings-title">Contract Warnings</div>
        <ul>
          {contract.meta.warnings.map((warning) => (
            <li key={warning}>{warning}</li>
          ))}
        </ul>
      </section>

      <main className="app-main">{children}</main>
    </div>
  );
};
