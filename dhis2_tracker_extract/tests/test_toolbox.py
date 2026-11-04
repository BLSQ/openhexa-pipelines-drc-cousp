"""Tests des transformations pures de ``toolbox`` (sans instance DHIS2).

On couvre le décodage ``value_norm`` (option set simple + choix multiples) et
les jointures d'enrichissement, qui portent la logique métier.
"""

from __future__ import annotations

import polars as pl

import toolbox


def _stage_de() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "program_stage_id": ["S1", "S1", "S1"],
            "program_stage_name": ["Notif", "Notif", "Notif"],
            "data_element_id": ["de_sexe", "de_signes", "de_temp"],
            "data_element_name": ["Sexe", "Symptômes", "Température"],
            "value_type": ["TEXT", "MULTI_TEXT", "NUMBER"],
            "option_set_id": ["os_sexe", "os_signes", None],
            "option_set_name": ["Sexe", "Symptômes", None],
        }
    )


def _options() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "option_set_id": ["os_sexe", "os_sexe", "os_signes", "os_signes"],
            "option_set_name": ["Sexe", "Sexe", "Symptômes", "Symptômes"],
            "option_id": ["o1", "o2", "o3", "o4"],
            "option_code": ["M", "F", "FIEV", "DIAR"],
            "option_name": ["Masculin", "Féminin", "Fièvre", "Diarrhée"],
        }
    )


def test_build_value_decoder_identifie_multi_text():
    de_map, multi = toolbox.build_value_decoder(_stage_de(), _options())
    assert de_map["de_sexe"]["M"] == "Masculin"
    assert "de_signes" in multi
    assert "de_sexe" not in multi
    # Un DE sans option set n'apparaît pas dans la table de décodage.
    assert "de_temp" not in de_map


def test_add_value_norm_decode_simple_et_multi():
    de_map, multi = toolbox.build_value_decoder(_stage_de(), _options())
    events = pl.DataFrame(
        {
            "data_element_id": ["de_sexe", "de_signes", "de_temp", "de_sexe"],
            "value": ["M", "FIEV,DIAR", "38.5", None],
        }
    )
    out = toolbox.add_value_norm(events, de_map, multi)["value_norm"].to_list()
    assert out == ["Masculin", "Fièvre, Diarrhée", "38.5", None]


def test_add_value_norm_table_vide():
    out = toolbox.add_value_norm(pl.DataFrame(), {}, set())
    assert "value_norm" in out.columns


def test_enrich_events_jointures():
    events = pl.DataFrame(
        {
            "event_id": ["e1"],
            "enrollment_id": ["enr1"],
            "tracked_entity_id": ["tei1"],
            "program_stage_id": ["S1"],
            "data_element_id": ["de_sexe"],
            "organisation_unit_id": ["ou1"],
            "value": ["M"],
            "value_norm": ["Masculin"],
        }
    )
    enrollments = pl.DataFrame(
        {
            "enrollment_id": ["enr1"],
            "enrolled_at": [pl.datetime(2026, 5, 1)],
            "enrollment_org_unit": ["ou1"],
        }
    )
    tei = pl.DataFrame({"tracked_entity_id": ["tei1"], "num_epid": ["RDC-001"]})
    org_levels = pl.DataFrame({"id": ["ou1"], "level_2_name": ["Nord-Kivu"]})

    out = toolbox.enrich_events(events, enrollments, tei, _stage_de(), org_levels)
    row = out.to_dicts()[0]
    assert row["enrollment_org_unit"] == "ou1"
    assert row["num_epid"] == "RDC-001"
    assert row["data_element_name"] == "Sexe"
    assert row["level_2_name"] == "Nord-Kivu"


class _FakeApi:
    """Simule ``tracker.api.get`` : sert ``data[endpoint]`` filtré et paginé."""

    def __init__(self, data: dict[str, list[dict]], id_keys: dict[str, str]):
        self.data = data
        self.id_keys = id_keys
        self.calls: list[tuple[str, dict]] = []

    def get(self, endpoint: str, params: dict) -> dict:
        self.calls.append((endpoint, dict(params)))
        key = endpoint.split("/")[-1]
        items = self.data[endpoint]
        for name in (key, self.id_keys[endpoint]):
            if name in params:
                wanted = set(params[name].split(";"))
                items = [i for i in items if i[self.id_keys[endpoint]] in wanted]
        size, page = params["pageSize"], params["page"]
        return {key: items[(page - 1) * size : page * size], "pager": {"page": page}}


class _FakeTracker:
    def __init__(self, api: _FakeApi, version: str = "2.41.6"):
        self.api = api
        self.version = version


def _tei(uid: str) -> dict:
    return {
        "trackedEntity": uid,
        "orgUnit": "ou1",
        "createdAt": "2026-01-01T00:00:00",
        "updatedAt": "2026-01-02T00:00:00",
        "attributes": [{"displayName": "Sexe ", "value": "M"}],
    }


def _fake(n: int, version: str = "2.41.6") -> _FakeTracker:
    api = _FakeApi(
        {
            "tracker/trackedEntities": [_tei(f"t{i}") for i in range(n)],
            "tracker/enrollments": [
                {"enrollment": f"e{i}", "enrolledAt": "2026-01-01", "orgUnit": "ou1"}
                for i in range(n)
            ],
        },
        {"tracker/trackedEntities": "trackedEntity", "tracker/enrollments": "enrollment"},
    )
    return _FakeTracker(api, version)


def test_get_tracked_entities_pagination_complete():
    tracker = _fake(7)
    df = toolbox.get_tracked_entities(tracker, "P", page_size=3)
    assert df.height == 7
    assert df["tracked_entity_id"].n_unique() == 7
    # 3 + 3 + 1 : la page incomplète clôt la boucle, sans requête vide en plus.
    assert [c[1]["page"] for c in tracker.api.calls] == [1, 2, 3]
    assert "Sexe" in df.columns


def test_get_tracked_entities_filtre_par_uid_et_par_lots(monkeypatch):
    monkeypatch.setattr(toolbox.config, "UID_BATCH_SIZE", 2)
    tracker = _fake(10)
    df = toolbox.get_tracked_entities(tracker, "P", tracked_entity_ids=["t1", "t4", "t8"])
    assert sorted(df["tracked_entity_id"].to_list()) == ["t1", "t4", "t8"]
    sent = [c[1]["trackedEntities"] for c in tracker.api.calls]
    assert sent == ["t1;t4", "t8"]


def test_get_enrollments_liste_vide_aucun_appel():
    tracker = _fake(5)
    df = toolbox.get_enrollments(tracker, "P", enrollment_ids=[])
    assert df.is_empty()
    assert tracker.api.calls == []


def test_parametres_avant_2_41():
    tracker = _fake(5, version="2.40.5")
    toolbox.get_enrollments(tracker, "P", enrollment_ids=["e2"])
    params = tracker.api.calls[0][1]
    assert params["enrollment"] == "e2"
    assert params["ouMode"] == "DESCENDANTS"
    assert "enrollments" not in params
    assert "orgUnitMode" not in params


def test_parametres_a_partir_de_2_41():
    tracker = _fake(5)
    toolbox.get_enrollments(tracker, "P", enrollment_ids=["e2"])
    params = tracker.api.calls[0][1]
    assert params["enrollments"] == "e2"
    assert params["orgUnitMode"] == "DESCENDANTS"


def test_reponse_instances_avant_2_41():
    class _Api:
        def get(self, endpoint, params):
            return {"instances": [{"event": "ev1", "dataValues": []}], "page": 1}

    tracker = _FakeTracker(_Api(), version="2.39.3")
    df = toolbox.get_events(tracker, "P", page_size=10)
    assert df["event_id"].to_list() == ["ev1"]


def test_check_dhis2_version():
    import pytest

    assert toolbox.check_dhis2_version(_FakeTracker(None, "2.38.0")) == "2.38.0"
    with pytest.raises(ValueError):
        toolbox.check_dhis2_version(_FakeTracker(None, "2.37.9"))
    with pytest.raises(ValueError):
        toolbox.check_dhis2_version(_FakeTracker(None, ""))


def test_slugify():
    assert toolbox.slugify("MVE - Fiche Contacts (Été)") == "mve_fiche_contacts_ete"
    assert len(toolbox.slugify("x" * 100)) == 47


def _programs_tracker(programs: list[dict]) -> _FakeTracker:
    class _Api:
        def get(self, endpoint, params):
            return {"programs": programs}

    return _FakeTracker(_Api())


def test_get_programs_noms_historiques_et_shortname():
    tracker = _programs_tracker(
        [
            {"id": "wDFB7FFlYQM", "name": "x", "programType": "WITH_REGISTRATION"},
            {
                "id": "NEW",
                "name": "Rougeole",
                "shortName": "Rougeole - Cas",
                "programType": "WITH_REGISTRATION",
            },
        ]
    )
    out = toolbox.get_programs(tracker, ["wDFB7FFlYQM", "NEW"])
    assert out == {"wDFB7FFlYQM": "mve_notification", "NEW": "rougeole_cas"}
    assert all(len(name) <= 47 for name in out.values())


def test_get_programs_erreurs():
    import pytest

    event_program = [{"id": "EV", "name": "Ev", "programType": "WITHOUT_REGISTRATION"}]
    with pytest.raises(ValueError, match="introuvables"):
        toolbox.get_programs(_programs_tracker([]), ["ABSENT"])
    with pytest.raises(ValueError, match="événementiels"):
        toolbox.get_programs(_programs_tracker(event_program), ["EV"])
