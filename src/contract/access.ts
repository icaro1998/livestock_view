import { contract } from "./contract.generated";
import type { Endpoint, Role } from "./types";

type RequiredRole = NonNullable<Endpoint["required_role"]>;

const normalizeRequiredRole = (required: RequiredRole): Role => {
  if (required === "admin (unless bootstrap allowed)") return "admin";
  return required;
};

export const roleAllows = (role: Role, required: RequiredRole) => {
  const hierarchy = contract.roles_acl.hierarchy;
  const normalized = normalizeRequiredRole(required);
  return hierarchy[role] >= hierarchy[normalized];
};
