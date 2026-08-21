from molgenis_fdp_harvester.utils import get_missing_required_fields, get_record_label


def test_get_record_label_uses_title_for_dataset():
    assert get_record_label({"title": "My Dataset"}, "dataset") == "My Dataset"


def test_get_record_label_uses_name_for_publisher():
    """Publishers (Agents) have no 'title' field, unlike Datasets - they use 'name'."""
    assert get_record_label({"name": "Acme Org"}, "publisher") == "Acme Org"


def test_get_record_label_uses_fn_for_kind():
    assert get_record_label({"fn": "Jane Doe"}, "kind") == "Jane Doe"


def test_get_record_label_uses_label_for_provenancestatement():
    assert get_record_label({"label": "Collected manually"}, "provenancestatement") == "Collected manually"


def test_get_record_label_falls_back_when_primary_field_missing():
    assert get_record_label({"id": "abc-123"}, "publisher") == "abc-123"


def test_get_record_label_prefers_uri_over_munged_id():
    """The id is a munged slug; the URI still points at the record in the source FDP."""
    concept = {"uri": "http://example.com/publisher1", "id": "http-example-com-publisher1"}

    assert get_record_label(concept, "publisher") == "http://example.com/publisher1"


def test_get_record_label_falls_back_to_unidentified_when_nothing_present():
    assert get_record_label({}, "publisher") == "<unidentified publisher>"


def test_get_record_label_handles_missing_concept_type():
    assert get_record_label({"title": "Fallback title"}) == "Fallback title"


def test_get_missing_required_fields_reports_missing_title_for_dataset():
    assert get_missing_required_fields({"id": "x"}, "dataset") == ["title"]


def test_get_missing_required_fields_reports_missing_name_for_publisher():
    assert get_missing_required_fields({"uri": "http://x"}, "publisher") == ["name"]


def test_get_missing_required_fields_empty_when_all_present():
    assert get_missing_required_fields({"title": "x"}, "dataset") == []


def test_get_missing_required_fields_unknown_concept_type_has_no_requirements():
    assert get_missing_required_fields({}, "unknown_type") == []


def test_get_missing_required_fields_treats_empty_string_as_missing():
    assert get_missing_required_fields({"title": ""}, "dataset") == ["title"]
