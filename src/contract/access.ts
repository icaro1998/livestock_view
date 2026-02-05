import { contract } from "./contract.generated";
import type { Role } from "./types";

export const roleAllows = (role: Role, required: Role) => {
  const hierarchy = contract.roles_acl.hierarchy;
  return hierarchy[role] >= hierarchy[required];
};
