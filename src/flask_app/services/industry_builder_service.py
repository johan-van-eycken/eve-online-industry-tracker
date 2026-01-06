from __future__ import annotations

from typing import Any, Dict, List


def enrich_blueprints_for_character(blueprints: List[Dict[str, Any]], character) -> List[Dict[str, Any]]:
    """Apply character-skill requirements and basic cost/value analysis.

    Intentionally keeps the existing blueprint payload shape stable.
    """
    char_skills = (getattr(character, "skills", None) or {}).get("skills", []) or []

    for bp in blueprints:
        required_skills = bp.get("required_skills", [])
        skill_requirements_met = True

        for skill in required_skills:
            skill_type_id = skill.get("type_id")
            required_level = skill.get("level", 0)
            char_skill = next((s for s in char_skills if s.get("skill_id") == skill_type_id), None)
            char_level = char_skill.get("trained_skill_level", 0) if char_skill else 0

            skill["character_level"] = char_level
            skill["skill_requirement_met"] = char_level >= required_level

            if char_level < required_level:
                skill_requirements_met = False

        bp["skill_requirements_met"] = skill_requirements_met

    for bp in blueprints:
        total_material_cost = 0.0
        total_product_value = 0.0

        me_level = bp.get("blueprint_material_efficiency", 0) or 0
        me_reduction = 1.0 - (me_level * 0.01)

        for mat in bp.get("materials", []):
            base_qty = mat.get("quantity", 0)
            adjusted_qty = int(base_qty * me_reduction)
            mat_price = mat.get("adjusted_price", 0.0) or 0.0
            total_material_cost += adjusted_qty * mat_price
            mat["adjusted_quantity"] = adjusted_qty

        bp["total_material_cost"] = total_material_cost

        for prod in bp.get("products", []):
            prod_qty = prod.get("quantity", 0)
            prod_price = prod.get("average_price", 0.0) or 0.0
            total_product_value += prod_qty * prod_price

        bp["total_product_value"] = total_product_value
        bp["profit_margin"] = total_product_value - total_material_cost
        bp["me_reduction_factor"] = me_reduction
        bp["me_savings_percentage"] = me_level

    return blueprints
