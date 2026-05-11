"""ConfigProducer for evolution tables."""


def produce_configs(spec: dict, minimal: bool = False) -> list[dict]:
    """Produce table configs from the evolution spec YAML.

    Returns the evolution_tables list directly (already declarative).
    """
    tables = list(spec.get('evolution_tables', []))

    if minimal:
        # Keep only one table per category
        seen_categories = set()
        filtered = []
        for t in tables:
            cat = t.get('category', '')
            if cat not in seen_categories:
                seen_categories.add(cat)
                filtered.append(t)
        tables = filtered

    return tables
