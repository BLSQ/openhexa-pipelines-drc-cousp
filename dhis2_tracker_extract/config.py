from __future__ import annotations

PROGRAMS: dict[str, str] = {
    "wDFB7FFlYQM": "mve_notification",  # Notification d'alerte et investigation de cas
    "wyLS271t8Id": "mve_contacts",  # Fiche d'identification et suivi individuel des contacts
}

ORG_UNIT_PARENT = "ymGeqzoPhN3"

PAGE_SIZE = 1000

TEI_PAGE_SIZE = 250

UID_BATCH_SIZE = 200

EVENT_FIELDS = (
    "event,status,program,trackedEntity,programStage,enrollment,orgUnit,"
    "occurredAt,createdAt,updatedAt,deleted,dataValues[dataElement,value]"
)

UPSERT_CHUNK_SIZE = 200_000

LOG_EVERY_PAGES = 10

MIN_DHIS2_MINOR = 38

MULTI_TEXT_TYPE = "MULTI_TEXT"

UPSERT_KEY = "event_id"

DELETED_FLAGS = ("deleted", "enrollment_deleted")

STATE_TABLE = "dhis2_extract_state"
