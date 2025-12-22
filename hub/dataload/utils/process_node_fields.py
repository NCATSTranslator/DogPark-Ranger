from typing import Any


def parse_bool(val: Any) -> bool:
    """parse str and int into bool"""
    if isinstance(val, bool):
        return val

    if isinstance(val, int):
        if val in (0, 1):
            return bool(val)
        raise ValueError(f"Invalid int for bool: {val}")

    if isinstance(val, str):
        val = val.strip().lower()
        if val in {"true", "1", "yes", "y", "on"}:
            return True
        if val in {"false", "0", "no", "n", "off"}:
            return False
        raise ValueError(f"invalid boolean string: {val}")

    raise ValueError(f"can't parse to bool: {val}")

def process_chembl_black_box_warning(node):
    """processor for DINGO datasets, where `category` is already a list"""
    field_value = node.get("chembl_black_box_warning", None)

    if field_value is not None \
            and not isinstance(field_value, bool):
        node["chembl_black_box_warning"] = parse_bool(field_value)


    return node