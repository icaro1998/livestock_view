import hashlib
import re
from pathlib import Path

CONTRACT_PATH = Path("docs/CONTRACT_PACK.json")
GENERATED_PATH = Path("src/contract/contract.generated.ts")


def main() -> None:
    if not CONTRACT_PATH.exists():
        raise SystemExit(f"Missing {CONTRACT_PATH}")
    if not GENERATED_PATH.exists():
        raise SystemExit(f"Missing {GENERATED_PATH}. Run scripts/generate-contract-types.py")

    raw = CONTRACT_PATH.read_bytes()
    contract_hash = hashlib.sha256(raw).hexdigest()
    text = GENERATED_PATH.read_text(encoding="utf-8")

    match = re.search(r"CONTRACT_HASH\s*=\s*\"([a-f0-9]+)\"", text)
    if not match:
        raise SystemExit("Could not find CONTRACT_HASH in generated types")

    embedded = match.group(1)
    if embedded != contract_hash:
        raise SystemExit(
            "Contract hash mismatch. Run scripts/generate-contract-types.py to regenerate.\n"
            f"Expected {contract_hash} but found {embedded}."
        )

    print("Contract hash matches generated types")


if __name__ == "__main__":
    main()
