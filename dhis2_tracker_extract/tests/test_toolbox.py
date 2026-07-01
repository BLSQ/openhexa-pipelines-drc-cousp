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
