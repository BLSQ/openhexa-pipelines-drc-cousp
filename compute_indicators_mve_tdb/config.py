# ruff: noqa: E501, RUF001
AXES_EXPORT: list[tuple[str, str]] = [
    ("date_notif", "COD_MVE_Tracker_Agg"),
    ("date_debut_symptomes", "COD_MVE_Tracker_DDS_Agg"),
    ("date_deces", "COD_MVE_Tracker_Deces"),
]

# Table de la liste de ligne nominative (grain cas)
LLN_TABLE = "COD_MVE_Tracker_Individu"

AGE_BINS = [0, 5, 15, 25, 45, 65, float("inf")]
AGE_LABELS = [
    "1. 0-4 ans",
    "2. 5-14 ans",
    "3. 15-24 ans",
    "4. 25-44 ans",
    "5. 45-64 ans",
    "6. 65+ ans",
]

EXPR_TEI = [
    "tracked_entity_id",
    "numero_epid",
    "secteur",
    "age_ans",
    "age_mois",
    "profession",
    "sexe",
    "date_debut_symptomes",
    "date_notification",
    "lien_epidemiologique",
]

COLS_PRELEV = [
    "tracked_entity_id",
    "lab_confirme",
    "date_confirmation",
    "lab_resultat_courant",
    "date_dernier_test",
    "n_tests_labo",
    "n_pos",
    "n_neg",
    "n_inv",
    "flag_pos_puis_neg",
]

DATE_COLS = {
    # nom_cible                    : source dans raw_df
    "date_notif": "date_notification",
    "date_debut_symptomes": "date_debut_symptomes",
    "date_debut_signes_invest": "date_debut_signes_investigation",
    "date_prelevement": "date_prelevement",
    "date_reception_labo": "date_reception_labo",
    "date_analyse_labo": "date_analyse_labo",
    "date_deces_final": "date_deces_final",
    "date_deces_notification": "date_deces_notification",
    "date_funerailles": "date_funerailles",
    "date_premiere_vaccination": "date_premiere_vaccination",
    "date_heure_investigation": "date_heure_investigation",
}

DICO_TEI = {
    "MVE - Numéro Epid - Alerte MVE": "numero_epid",
    "MVE - Secteur (spécifiez)": "secteur",
    "MVE - Age(ans)": "age_ans",
    "MVE - Age (Mois)": "age_mois",
    "MVE-N-Profession": "profession",
    "MVE-N-Sexe": "sexe",
    "MVE - DDS (Date de début des symptômes)": "date_debut_symptomes",
    "MPOX-N-Date et heure de notification de l'alerte": "date_notification",
    "MVE - Lien épidémiologique": "lien_epidemiologique",
}


RENAME_MAP = {
    # Enrollment
    "MVE - Numéro Epid - Alerte MVE": "numero_epid",
    # "MVE - Nom, post nom et prénom du cas": "nom_cas",
    "Organisation unit name hierarchy": "geo_hierarchie",
    "MVE - Secteur (spécifiez)": "secteur",
    "MVE - Age(ans)": "age_ans",
    "MVE - Age (Mois)": "age_mois",
    "MVE-N-Sexe": "sexe",
    "MVE-N-Profession": "profession",
    "MVE - DDS (Date de début des symptômes)": "date_debut_symptomes",
    "MPOX-N-Date et heure de notification de l'alerte": "date_notification",
    "MVE - Lien épidémiologique": "lien_epidemiologique",
    # Stage Notification
    "MVE-N Symptômes": "symptomes_notification",
    "MVE-N Nature de l'Alerte": "nature_alerte",
    "MVE-N-Conclusion de l'alerte": "conclusion_alerte",
    "MVE-N-Date & Heure d'investigation": "date_heure_investigation",
    "MVE- N - Date de décès": "date_deces_notification",
    # "MVE- N - Profession": "profession_notification",
    "MVE - N - Cas suspect": "classification_initiale",
    "MVE-N Etat de santé actuel": "etat_sante_notification",
    "MVE-N Commission PEC prévenue": "commission_pec_prevenue",
    # Stage Investigation — clinique
    "S1 -  Etat du patient au moment de la collecte d'information": "etat_patient_investigation",
    "042 - MVE - S2 - Date de début des signes et symptômes": "date_debut_signes_investigation",
    "043 - MVE - S2 - Fièvre": "signe_fievre",
    "044 - MVE - S2 - Si oui, Temp C  (Thermoflash)": "temperature_celsius",
    "045 - MVE - S2 - Nausées / Vomissements": "signe_nausees_vomissements",
    "046 - MVE - S2 - Diarrhées": "signe_diarrhees",
    "047 - MVE - S2 - Fatigue générale intense": "signe_fatigue",
    "053 - MVE - S2 - Céphalées": "signe_cephalees",
    "063 - MVE - S2 - Coma / perte de conscience": "signe_coma",
    "064 - MVE - S2 - Confusion ou désorientation": "signe_confusion",
    "065 - MVE - S2 - Saignements": "signe_saignements",
    "067 - MVE - S2 - Saignements des gencives": "signe_saignement_gencives",
    "069 - MVE - S2 - Saignements du nez (épistaxis)": "signe_epistaxis",
    "070 - MVE - S2 - Selles rouges ou noires (mélénas)": "signe_melenas",
    "071 - MVE - S2 - Vomissements sanglants (hématémèses)  ": "signe_hematemeses",
    "075 - MVE - S2 - Hématomes / Pétéchies / purpura  ": "signe_hematomes_petechies",
    # Investigation — hospitalisation
    "091 - MVE - S3 - HO-1 - Date d'hospitalisation/Date de consultation - Début": "date_hospitalisation_ho1",
    "093 - MVE - S3 - HO-1 - Nom de l'établissement de soins": "etablissement_ho1",
    "S1 -  Nom de l'établissement de soins": "etablissement_soins_s1",
    "094 - MVE - S3 - HO-1 - Zone de santé": "zone_sante_ho1",
    # Investigation — expositions
    "106 - MVE - S4 - 1. Il y a-t-il eu contacts avec un malade Ebola, connu/suspect, ou simplement avec une personne malade?": "contact_cas_ebola_connu",
    "115 - MVE - S4 - MA-1 - Types de contact": "types_contact",
    "108 - MVE - S4 - MA-1 - Lien de parenté": "lien_parente_cas_index",
    "177 - MVE - S4 - 6. Le patient a-t-il eu un contact direct (chasse, touché, mangé) avec des animaux ou de la viande crue avant de tomber malade?": "contact_animal",
    "177 - MVE - S4 - 6.1 Chauve-souris (ou excrétions de)": "contact_chauve_souris",
    "177 - MVE - S4 - 6.2 Singes": "contact_singes",
    "155 - MVE - S4 - PF-2 - Avez-vous porté ou touché le corps?": "touche_corps_funerailles",
    # Investigation — géolocalisation
    "MVE - S1 - Endroit où le patient est tombé malade : Zone de Santé": "zone_sante_maladie",
    "MVE - S1 -  Endroit où le patient est tombé malade : Village/Ville": "village_maladie",
    "S1 -  Coordonnées GPS de la maison": "gps_domicile",
    # Investigation — vaccination
    "S1 -  Statut vaccinal du malade: vacciné contre MVE": "statut_vaccinal_mve",
    "MVE - S1 - Combien de fois le patient a-t-il été vacciné contre d'Ebola ?": "nb_doses_vaccin",
    "MVE - S1 - Date de la première vaccination ?": "date_premiere_vaccination",
    "MVE - S1 -  La malade est-elle enceinte ?": "grossesse",
    # Stage Prélèvements biologiques
    "182 - MVE - S5 - Est-ce qu'un prélèvement a déjà été soumis pour ce malade?": "prelevement_soumis",
    "183 - MVE - S5 - Date du prélèvement": "date_prelevement",
    "184 - MVE - S5 - Type de prélèvement": "type_prelevement",
    "185 - MVE - S5 - PR - précisez": "type_prelevement_precision",
    "182.1 - MVE - S5 - Identifiant Labo": "identifiant_labo",
    "182.2 - MVE - S5 - Statut du patient lors du prélèvement": "statut_patient_prelevement",
    "MVE - N° Prélèvement": "numero_prelevement",
    # Stage Résultat Laboratoire
    "MVE - LAB - Date de Reception": "date_reception_labo",
    "MVE - LAB - Date d'analyse": "date_analyse_labo",
    "MVE - LAB - Résultat Final (MVE)": "resultat_final_mve",
    "MVE - Classification finale du cas": "classification_finale_cas",
    "MVE - LAB - Radi One – Ebola — Valeur CT fam (EBOV)": "valeur_ct_ebov",
    "MVE - LAB - Radi One – Ebola — Valeur CT HEC (IC)": "valeur_ct_hec",
    "MVE - LAB - Co-infection ?": "co_infection",
    "MVE - LAB - Si Co-infection": "co_infection_type",
    "MVE - LAB - Incident": "incident_labo",
    "MVE - LAB - Machine": "machine_labo",
    # Stage Statut final
    "199 - MVE - S6 - Statut final du patient": "statut_final_patient",
    "206 - MVE - S6 - Date de sortie de l'hôpital": "date_sortie_hopital",
    "208 - MVE - S6 - Date de décès": "date_deces_final",
    "209 - MVE - S6 - Lieu du décès": "lieu_deces",
    "212 - MVE - S6 - DC - Zone de Santé": "zone_sante_deces",
    "200 - MVE - S6 - Est-ce-que le patient a eu des signes hémorragiques inexpliqués pendant la durée de la maladie?": "signes_hemorragiques_maladie",
    "214 - MVE - S6 - Date des funérailles": "date_funerailles",
    "215 - MVE - S6 - Funérailles organisées par": "funerailles_organisees_par",
    "216 - MVE - S6 - Lieu des funérailles/enterrement": "lieu_funerailles",
    "218 - MVE - S6 - FE - Zone de Santé": "zone_sante_funerailles",
    "205 - MVE - S6 - Si le malade était en isolement, date de sortie de la zone d'isolement": "date_sortie_isolement",
    "204 - MVE - S6 - SG - Zone de santé": "zone_sante_guerison",
    # Stage Prise en charge — PEC_DISABLED
    # Ces entrées sont conservées pour le jour où le stage sera réactivé.
    # Elles n'auront aucun effet tant que le stage n'est pas dans ALL_DIMENSIONS.
    # "MVE - PEC - Date d'admission":                   "date_admission_cte",    # PEC_DISABLED
    # "MVE - PEC - Modalité de sortie":                  "modalite_sortie_cte",   # PEC_DISABLED
    # "MVE - PEC - Status avant admission":              "statut_avant_admission_cte",  # PEC_DISABLED
}


DICO_DE_MAPPING = {
    "symptomes_notification": "qdhYjojAAXd",
    "nature_alerte": "kdOYmDgoyAA",
    "conclusion_alerte": "KhsBtTYkFZd",
    "date_heure_investigation": "F0gpBf9R11P",
    "date_deces_notification": "ZBhXK4z0Iax",
    "classification_initiale": "jHaeHsB6JbW",
    "etat_sante_notification": "rEMVmX2CvRw",
    "commission_pec_prevenue": "rrFePJwactM",
    "etat_patient_investigation": "o7NC9z4JAts",
    "date_debut_signes_investigation": "aRju8gQZBET",
    "signe_fievre": "uW3XFH8TQGE",
    "temperature_celsius": "T3jzcNGXCpa",
    "signe_nausees_vomissements": "xATq2Gnt48G",
    "signe_diarrhees": "Pjk2zRsdLEv",
    "signe_fatigue": "g2QJ4LWuq1C",
    "signe_cephalees": "ZwlwHsvxPA3",
    "signe_coma": "vwS0SsOqCz9",
    "signe_confusion": "fjXyHX02I8c",
    "signe_saignements": "HrFOPwqKxoV",
    "signe_saignement_gencives": "pwNocbwvO0o",
    "signe_epistaxis": "N50wDaI6H1r",
    "signe_melenas": "BYkTKut1D8V",
    "signe_hematemeses": "Gutl308P6Pl",
    "signe_hematomes_petechies": "f0yTueLYdns",
    "date_hospitalisation_ho1": "MhWvM2jHEvL",
    "etablissement_ho1": "sRCOxZrDZkv",
    "etablissement_soins_s1": "cHbhxbwAZZ3",
    "zone_sante_ho1": "y8Yv0WaxsJA",
    "contact_cas_ebola_connu": "Tzr3SapM9je",
    "types_contact": "fRj81KZWlYh",
    "lien_parente_cas_index": "hOqgFC3f94P",
    "contact_animal": "PydxMCR9fV6",
    "contact_chauve_souris": "fq5cNcnKcy9",
    "contact_singes": "alu85ZZRCZE",
    "touche_corps_funerailles": "P8TAPKXAK2E",
    "zone_sante_maladie": "Fl9ty8UdhnJ",
    "village_maladie": "D41GBZFDn5t",
    "gps_domicile": "mMGawAScUbp",
    "statut_vaccinal_mve": "t4RcYSXmYgW",
    "nb_doses_vaccin": "dAIplu60XuM",
    "date_premiere_vaccination": "j4A3wbzVrWz",
    "prelevement_soumis": "aC7D1VntfwF",
    "date_prelevement": "CxQAC5LkMtn",
    "type_prelevement": "USnTDONKNN8",
    "type_prelevement_precision": "NT3xJOu8JAL",
    "identifiant_labo": "hRDXEdSBqNF",
    "statut_patient_prelevement": "nniQIfMGBDC",
    "numero_prelevement": "lj0Zv0vbUN5",
    "date_reception_labo": "HBw0c2Cg8GU",
    "date_analyse_labo": "BTMKxJvLTer",
    "resultat_final_mve": "j6xabrRDJuo",
    "classification_finale_cas": "D6kduc7OZnS",
    "valeur_ct_ebov": "DBdW3r069Yn",
    "valeur_ct_hec": "CBn9FhYHn0Y",
    "co_infection": "mRyo3TkE7jp",
    "co_infection_type": "q0aEkUpgpNh",
    "incident_labo": "Smg0g56IqWr",
    "machine_labo": "rtfha5Df5a8",
    "statut_final_patient": "Za0cx3pmcWW",
    "date_sortie_hopital": "wIY8Kv2oWec",
    "date_deces_final": "x1aazi4fgKO",
    "lieu_deces": "sHEARVNufMJ",
    "zone_sante_deces": "dqmYvLDGfDu",
    "signes_hemorragiques_maladie": "jieNzfUp3E8",
    "date_funerailles": "eLqoRcK7lq1",
    "funerailles_organisees_par": "fpw6gIG7Nhq",
    "lieu_funerailles": "LE2eGGkAy2F",
    "zone_sante_funerailles": "NympO1c3msQ",
    "date_sortie_isolement": "W2u38gg9Jy8",
    "zone_sante_guerison": "fg5xfl9bD5V",
    "grossesse": "ICpmsUy8ros",
}

# Délais (jours) : nom_cible -> (date_fin, date_debut)
DELAI_DEFS = {
    "delai_sympt_notif": ("date_notif", "date_debut_symptomes"),
    "delai_notif_prelev": ("date_prelevement", "date_notif"),
    "delai_prelev_reception": ("date_reception_labo", "date_prelevement"),
    "delai_recept_result": ("date_analyse_labo", "date_reception_labo"),
    "delai_notif_result": ("date_analyse_labo", "date_notif"),
    #  "duree_sejour_cte": ("date_sortie_cte", "date_admission_cte"),
}

# Bornes de plausibilité (jours) appliquées aux délais ; mêmes clés que DELAI_DEFS
DELAI_BORNES = {
    "delai_sympt_notif": (0, 21),  # incubation MVE max 21 j
    "delai_notif_prelev": (0, 7),  # objectif riposte < 24h
    "delai_prelev_reception": (0, 7),  # transport vers labo
    "delai_recept_result": (0, 7),  # réception → analyse
    "delai_notif_result": (0, 21),  # bout en bout
    #   "duree_sejour_cte":       (0, 42),  # séjour CTE
}

# Schéma de la liste de ligne nominative publiée (ordre des colonnes en sortie).
# Inclut les colonnes dérivées (délais, statut vital, labo) : la sélection se
# fait APRÈS dérivation dans build_line_list_individu().
LLN_COLS = [
    # ── Identité / géo / dates ───────────────────────────────────────────────
    "numero_epid",
    "date_notif",
    "semaine_epidemio",
    "date_debut_symptomes",
    "date_heure_investigation",
    "temperature_celsius",
    "date_prelevement",
    "date_reception_labo",
    "date_analyse_labo",
    "machine_labo",
    "date_deces",
    #  "date_admission_cte", "date_sortie_cte",
    "province",
    "zone_sante",
    "aire_sante",
    "tranche_age",
    "sexe_norm",
    # ── Drapeaux ─────────────────────────────────────────────────────────────
    "is_alerte",
    "is_alerte_valide",
    "is_suspect",
    "is_confirme",
    "is_preleve",
    "is_recu",
    "is_analyse",
    "is_valide",
    "is_deces",
    "is_gueri",
    "is_deces_confirme",
    "is_confirme_gueri",
    "is_confirme_vivant",
    # ── Délais (jours, bornés) ───────────────────────────────────────────────
    "delai_sympt_notif",
    "delai_notif_prelev",
    "delai_prelev_reception",
    "delai_recept_result",
    "delai_notif_result",
    # ── Statut & labo ────────────────────────────────────────────────────────
    "statut_vital",
    "resultat_labo",
    "numero_prelevement",
    "identifiant_labo",
    "valeur_ct_ebov",
    "valeur_ct_hec",
    "ct_ebov_classe",
    # ── Carto (anneaux de coordonnées — redondant par ligne, à externaliser) ──
    "coordinates_zs",
    "coordinates_province",
]
