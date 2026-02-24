import argparse
import os
from collections import defaultdict

import yaml

TECH_LEVEL_ATTR_ID = 422  # Dogma attribute "Tech Level"
TECH1_META_GROUP_ID = 1


def load_yaml(path: str):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Check whether any Tech I manufactured products require manufacturable intermediate materials "
            "(i.e., could create submanufacturing jobs)."
        )
    )
    parser.add_argument(
        "--sde-dir",
        default=os.path.join("database", "data", "tmp_sde"),
        help="Path to unpacked SDE YAML directory (default: database/data/tmp_sde)",
    )
    parser.add_argument(
        "--max-examples",
        type=int,
        default=25,
        help="Number of example products to print",
    )
    parser.add_argument(
        "--t1-mode",
        choices=["metagroup", "techlevel"],
        default="metagroup",
        help=(
            "How to classify Tech I products. 'metagroup' uses types.yaml metaGroupID==1 (preferred). "
            "'techlevel' uses typeDogma attribute 422 and treats missing values as Tech I."
        ),
    )
    args = parser.parse_args()

    sde_dir = os.path.abspath(args.sde_dir)
    blueprints_path = os.path.join(sde_dir, "blueprints.yaml")
    types_path = os.path.join(sde_dir, "types.yaml")
    type_dogma_path = os.path.join(sde_dir, "typeDogma.yaml")

    if not os.path.exists(blueprints_path):
        raise SystemExit(f"Missing {blueprints_path}")
    if not os.path.exists(type_dogma_path):
        raise SystemExit(f"Missing {type_dogma_path}")
    if args.t1_mode == "metagroup" and not os.path.exists(types_path):
        raise SystemExit(f"Missing {types_path} (required for --t1-mode metagroup)")

    tech_level_by_type: dict[int, int] = {}
    meta_group_by_type: dict[int, int] = {}

    if args.t1_mode == "techlevel":
        print(f"Loading {os.path.relpath(type_dogma_path)} ...")
        type_dogma = load_yaml(type_dogma_path) or {}
        for tid, entries in type_dogma.items():
            try:
                type_id = int(tid)
            except Exception:
                continue
            if not isinstance(entries, list):
                continue
            for e in entries:
                if not isinstance(e, dict):
                    continue
                if int(e.get("attributeID") or 0) == TECH_LEVEL_ATTR_ID:
                    try:
                        tech_level_by_type[type_id] = int(e.get("value"))
                    except Exception:
                        pass
                    break
    else:
        print(f"Loading {os.path.relpath(types_path)} ...")
        types = load_yaml(types_path) or {}
        # types.yaml is a large map: type_id -> dict
        if isinstance(types, dict):
            for tid, entry in types.items():
                try:
                    type_id = int(tid)
                except Exception:
                    continue
                if not isinstance(entry, dict):
                    continue
                mg = entry.get("metaGroupID")
                if mg is None:
                    continue
                try:
                    meta_group_by_type[type_id] = int(mg)
                except Exception:
                    pass

    print(f"Loading {os.path.relpath(blueprints_path)} ...")
    blueprints = load_yaml(blueprints_path) or {}

    manufacturable_products: set[int] = set()
    manufacturing_materials_by_product: dict[int, list[dict]] = defaultdict(list)

    for bp in blueprints.values():
        if not isinstance(bp, dict):
            continue
        acts = bp.get("activities")
        if not isinstance(acts, dict):
            continue
        mfg = acts.get("manufacturing")
        if not isinstance(mfg, dict):
            continue
        products = mfg.get("products")
        if not isinstance(products, list) or not products:
            continue
        mats = mfg.get("materials")
        mats_list = mats if isinstance(mats, list) else []

        for p in products:
            if not isinstance(p, dict):
                continue
            try:
                prod_tid = int(p.get("typeID") or 0)
            except Exception:
                continue
            if prod_tid <= 0:
                continue
            manufacturable_products.add(prod_tid)
            if mats_list:
                manufacturing_materials_by_product[prod_tid].extend([mm for mm in mats_list if isinstance(mm, dict)])

    if args.t1_mode == "techlevel":
        # Important: missing tech-level dogma values are treated as Tech I in this mode.
        tech1_products = [t for t in manufacturable_products if tech_level_by_type.get(t, 1) == 1]
    else:
        # Only count items explicitly marked as metaGroupID==1.
        tech1_products = [t for t in manufacturable_products if meta_group_by_type.get(t) == TECH1_META_GROUP_ID]

    products_with_intermediates: list[tuple[int, list[int]]] = []
    for prod_tid in tech1_products:
        mats = manufacturing_materials_by_product.get(prod_tid) or []
        mat_type_ids: list[int] = []
        for mm in mats:
            try:
                mid = int(mm.get("typeID") or 0)
            except Exception:
                continue
            if mid > 0:
                mat_type_ids.append(mid)
        intermediates = sorted({mid for mid in mat_type_ids if mid in manufacturable_products})
        if intermediates:
            products_with_intermediates.append((prod_tid, intermediates))

    products_with_intermediates.sort(key=lambda x: x[0])

    print()
    print(f"Manufacturable products (any tech): {len(manufacturable_products)}")
    print(f"Tech I manufacturable products ({args.t1_mode}): {len(tech1_products)}")
    print(f"Tech I products with manufacturable intermediates: {len(products_with_intermediates)}")

    if products_with_intermediates:
        print()
        print("Examples (product_type_id -> some intermediate material type_ids):")
        for prod_tid, intermediates in products_with_intermediates[: max(0, int(args.max_examples))]:
            preview = ", ".join(str(x) for x in intermediates[:8])
            if len(intermediates) > 8:
                preview += ", …"
            print(f"- {prod_tid} requires: {preview}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
