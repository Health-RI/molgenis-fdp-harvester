class HarvesterException(Exception):
    pass


# Each concept type identifies itself with a different field: a Publisher (Agent) has no
# 'title' at all, so logging must not assume any single field is present.
_LABEL_FIELD_BY_CONCEPT_TYPE = {
    "dataset": "title",
    "datasetseries": "title",
    "publisher": "name",
    "kind": "fn",
    "provenancestatement": "label",
}

# 'uri' before 'id': a missing id gets munged into a slug, while the URI still points at
# the record in the source FDP.
_LABEL_FIELD_FALLBACKS = ("title", "name", "fn", "label", "uri", "id")

# 'id' is deliberately not required: the harvester derives it from the guid when absent.
REQUIRED_FIELDS_BY_CONCEPT_TYPE = {
    "dataset": ("title",),
    "datasetseries": ("title",),
    "publisher": ("name",),
    "kind": ("fn",),
    "provenancestatement": ("label",),
}


def get_record_label(concept_dict: dict, concept_type: str | None = None) -> str:
    """Return a human-readable label for a concept dict, for use in log messages."""
    concept_type = concept_type or ""
    primary_field = _LABEL_FIELD_BY_CONCEPT_TYPE.get(concept_type)
    if primary_field and concept_dict.get(primary_field):
        return concept_dict[primary_field]

    for field in _LABEL_FIELD_FALLBACKS:
        if concept_dict.get(field):
            return concept_dict[field]

    return f"<unidentified {concept_type or 'record'}>"


def get_missing_required_fields(concept_dict: dict, concept_type: str | None) -> list:
    """Return the required fields for the given concept type that are missing or empty."""
    required_fields = REQUIRED_FIELDS_BY_CONCEPT_TYPE.get(concept_type or "", ())
    return [field for field in required_fields if not concept_dict.get(field)]
