# ruff: noqa: E501, RUF001
# Table source (événements du tracker, format long) lue dans la base du workspace
EVENTS_TABLE = "mve_notification_events"

# Colonnes témoins du contrôle post-écriture (celles dont la cardinalité est
# connue et stable : géographie, identifiant de cas, tranche d'âge).
COLS_TEMOINS = ("province", "zone_sante", "aire_sante", "numero_epid", "tranche_age")

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
    "date_deces_pci": "date_deces_pci",
    "date_funerailles": "date_funerailles",
    "date_premiere_vaccination": "date_premiere_vaccination",
    "date_heure_investigation": "date_heure_investigation",
    "date_admission_cte": "date_admission_cte",
    "date_sortie_cte": "date_sortie_cte",
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
    # Stage Prise en charge
    "MVE - PEC - Date d’admission": "date_admission_cte",
    "MVE - PEC - Date de sortie": "date_sortie_cte",
    "MVE - PEC - Status avant admission": "statut_avant_admission_cte",
    # Modalité de sortie du CTE : le DE s'appelle « Statut au moment de la sortie »
    # (modalités observées : Guéri(e), Décédé(e), Non Cas, Retour à la maison,
    # Abandon, Evadé(e), Référé, Transféré(e)).
    "MEV - PEC37 - Statut au moment de la sortie": "modalite_sortie_cte",
    # Prévention et contrôle des infections (PCI)
    "MVE - PCI9 - Date de décès": "date_deces_pci",
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
    "date_admission_cte": "KGsTJ4jV7Fb",
    "date_sortie_cte": "Xy5J5MGpaZ7",
    "statut_avant_admission_cte": "AawdHKqaXcj",
    "modalite_sortie_cte": "WKZu0kp6wWu",
    "date_deces_pci": "fWBGDpJezOX",
}

# Délais (jours) : nom_cible -> (date_fin, date_debut)
DELAI_DEFS = {
    "delai_sympt_notif": ("date_notif", "date_debut_symptomes"),
    "delai_notif_prelev": ("date_prelevement", "date_notif"),
    "delai_prelev_reception": ("date_reception_labo", "date_prelevement"),
    "delai_recept_result": ("date_analyse_labo", "date_reception_labo"),
    "delai_notif_result": ("date_analyse_labo", "date_notif"),
    "duree_sejour_cte": ("date_sortie_cte", "date_admission_cte"),
}

# Bornes de plausibilité (jours) appliquées aux délais ; mêmes clés que DELAI_DEFS
DELAI_BORNES = {
    "delai_sympt_notif": (0, 21),  # incubation MVE max 21 j
    "delai_notif_prelev": (0, 7),  # objectif riposte < 24h
    "delai_prelev_reception": (0, 7),  # transport vers labo
    "delai_recept_result": (0, 7),  # réception → analyse
    "delai_notif_result": (0, 21),  # bout en bout
    "duree_sejour_cte": (0, 42),  # séjour CTE
}


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
    "date_admission_cte",
    "date_sortie_cte",
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
    "is_resultat_valide",
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
    "duree_sejour_cte",
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

DATASET_LLN_MAPPING = {
    # Noms alignés sur DICO_DE_MAPPING (vocabulaire de la table TDB) pour les DE
    # communs aux deux dictionnaires. La table (COD_MVE_Tracker_Individu) n'est
    # pas touchée : DICO_DE_MAPPING/RENAME_MAP restent inchangés, donc les
    # tableaux de bord connectés à la table ne sont pas affectés.
    # Deux exceptions volontaires (le nom de la table pour ce DE n'est pas une
    # colonne DE→valeur brute mais un champ reconstruit à partir de plusieurs
    # DE — reprendre ce nom créerait une fausse équivalence) :
    #   - j6xabrRDJuo : "resultat_labo" déjà identique au nom réellement publié
    #     dans la table (alias direct de resultat_final_mve, cf. pipeline.py) ;
    #   - x1aazi4fgKO : gardé sous le nom brut DICO "date_deces_final" plutôt que
    #     "date_deces", qui dans la table désigne la date reconstruite
    #     (coalesce date_deces_final / date_deces_notification / date_deces_pci
    #     / proxys) et n'est donc pas équivalente à ce DE seul.
    # Stage 1 · Notification de l'alerte (njs9IDEFVtC)
    "kdOYmDgoyAA": "nature_alerte",  # MVE-N Nature de l'Alerte
    "rEMVmX2CvRw": "etat_sante_notification",  # MVE-N Etat de santé actuel
    "qdhYjojAAXd": "symptomes_notification",  # MVE-N Symptômes
    "F0gpBf9R11P": "date_heure_investigation",  # MVE-N-Date & Heure d'investigation
    "KhsBtTYkFZd": "conclusion_alerte",  # MVE-N-Conclusion de l'alerte
    "rrFePJwactM": "commission_pec_prevenue",  # MVE-N Commission PEC prévenue
    "jHaeHsB6JbW": "classification_initiale",  # MVE - N - Cas suspect
    "ZBhXK4z0Iax": "date_deces_notification",  # MVE- N - Date de décès
    # Stage 2 · Investigation FHV (fqrtWpCo7za)
    "mMGawAScUbp": "gps_domicile",  # S1 -  Coordonnées GPS de la maison
    "o7NC9z4JAts": "etat_patient_investigation",  # S1 -  Etat du patient au moment de la collecte d'information
    "cHbhxbwAZZ3": "etablissement_soins_s1",  # S1 -  Nom de l’établissement de soins  # noqa: RUF003
    "ICpmsUy8ros": "grossesse",  # MVE - S1 -  La malade est-elle enceinte ?
    "t4RcYSXmYgW": "statut_vaccinal_mve",  # S1 -  Statut vaccinal du malade: vacciné contre MVE
    "dAIplu60XuM": "nb_doses_vaccin",  # MVE - S1 - Combien de fois le patient a-t-il été vacciné contre d’Ebola ?  # noqa: RUF003
    "j4A3wbzVrWz": "date_premiere_vaccination",  # MVE - S1 - Date de la première vaccination ?
    "D41GBZFDn5t": "village_maladie",  # MVE - S1 -  Endroit où le patient est tombé malade : Village/Ville
    "Fl9ty8UdhnJ": "zone_sante_maladie",  # MVE - S1 - Endroit où le patient est tombé malade : Zone de Santé
    # NB : distinct de l'attribut TEI « date_debut_symptomes » (MVE - DDS) pour
    # éviter toute collision à la jointure ; nom aligné sur DICO_DE_MAPPING.
    "aRju8gQZBET": "date_debut_signes_investigation",  # 042 - MVE - S2 - Date de début des signes et symptômes
    "uW3XFH8TQGE": "signe_fievre",  # 043 - MVE - S2 - Fièvre
    "T3jzcNGXCpa": "temperature_celsius",  # 044 - MVE - S2 - Si oui, Temp C  (Thermoflash)
    "xATq2Gnt48G": "signe_nausees_vomissements",  # 045 - MVE - S2 - Nausées / Vomissements
    "Pjk2zRsdLEv": "signe_diarrhees",  # 046 - MVE - S2 - Diarrhées
    "g2QJ4LWuq1C": "signe_fatigue",  # 047 - MVE - S2 - Fatigue générale intense
    "ZwlwHsvxPA3": "signe_cephalees",  # 053 - MVE - S2 - Céphalées
    "vwS0SsOqCz9": "signe_coma",  # 063 - MVE - S2 - Coma / perte de conscience
    "fjXyHX02I8c": "signe_confusion",  # 064 - MVE - S2 - Confusion ou désorientation
    "HrFOPwqKxoV": "signe_saignements",  # 065 - MVE - S2 - Saignements
    "pwNocbwvO0o": "signe_saignement_gencives",  # 067 - MVE - S2 - Saignements des gencives
    "N50wDaI6H1r": "signe_epistaxis",  # 069 - MVE - S2 - Saignements du nez (épistaxis)
    "BYkTKut1D8V": "signe_melenas",  # 070 - MVE - S2 - Selles rouges ou noires (mélénas)
    "Gutl308P6Pl": "signe_hematemeses",  # 071 - MVE - S2 - Vomissements sanglants (hématémèses)
    "f0yTueLYdns": "signe_hematomes_petechies",  # 075 - MVE - S2 - Hématomes / Pétéchies / purpura
    "MhWvM2jHEvL": "date_hospitalisation_ho1",  # 097 - MVE - S3 - HO-4 - Date d’hospitalisation/Date de consultation - Début  # noqa: RUF003
    "sRCOxZrDZkv": "etablissement_ho1",  # 093 - MVE - S3 - HO-1 - Nom de l’établissement de soins  # noqa: RUF003
    "y8Yv0WaxsJA": "zone_sante_ho1",  # 094 - MVE - S3 - HO-1 - Zone de santé
    "Tzr3SapM9je": "contact_cas_ebola_connu",  # 106 - MVE - S4 - 1. Il y a-t-il eu contacts avec un malade Ebola, connu/suspect, ou simplement avec une personne malade?
    "fRj81KZWlYh": "types_contact",  # 115 - MVE - S4 - MA-1 - Types de contact
    "hOqgFC3f94P": "lien_parente_cas_index",  # 108 - MVE - S4 - MA-1 - Lien de parenté
    "P8TAPKXAK2E": "touche_corps_funerailles",  # 155 - MVE - S4 - PF-2 - Avez-vous porté ou touché le corps?
    "PydxMCR9fV6": "contact_animal",  # 177 - MVE - S4 - 6. Le patient a-t-il eu un contact direct (chasse, touché, mangé) avec des animaux ou de la viande crue avant de tomber malade?
    "fq5cNcnKcy9": "contact_chauve_souris",  # 177 - MVE - S4 - 6.1 Chauve-souris (ou excrétions de)
    "alu85ZZRCZE": "contact_singes",  # 177 - MVE - S4 - 6.2 Singes
    # Stage 3 · Prélèvements biologiques (GO2aLxqhDIS)
    "lj0Zv0vbUN5": "numero_prelevement",  # MVE - N° Prélèvement
    "aC7D1VntfwF": "prelevement_soumis",  # 182 - MVE - S5 - Est-ce qu’un prélèvement a déjà été soumis pour ce malade?  # noqa: RUF003
    "hRDXEdSBqNF": "identifiant_labo",  # 182.1 - MVE - S5 - Identifiant Labo
    "nniQIfMGBDC": "statut_patient_prelevement",  # 182.2 - MVE - S5 - Statut du patient lors du prélèvement
    "CxQAC5LkMtn": "date_prelevement",  # 183 - MVE - S5 - Date du prélèvement
    "USnTDONKNN8": "type_prelevement",  # 184 - MVE - S5 - Type de prélèvement
    "NT3xJOu8JAL": "type_prelevement_precision",  # 185 - MVE - S5 - PR - précisez
    # Stage 4 · Résultat laboratoire (r7nrCHTBI5P)
    "HBw0c2Cg8GU": "date_reception_labo",  # MVE - LAB - Date de Reception
    "BTMKxJvLTer": "date_analyse_labo",  # MVE - LAB - Date d'analyse
    "rtfha5Df5a8": "machine_labo",  # MVE - LAB - Machine
    "j6xabrRDJuo": "resultat_labo",  # MVE - LAB - Résultat Final (MVE) — déjà le nom publié dans la table, cf. note plus haut
    "D6kduc7OZnS": "classification_finale_cas",  # MVE - Classification finale du cas
    "DBdW3r069Yn": "valeur_ct_ebov",  # MVE - LAB - Radi One – Ebola — Valeur CT fam (EBOV)  # noqa: RUF003
    "CBn9FhYHn0Y": "valeur_ct_hec",  # MVE - LAB - Radi One – Ebola — Valeur CT HEC (IC)  # noqa: RUF003
    "mRyo3TkE7jp": "co_infection",  # MVE - LAB - Co-infection ?
    "q0aEkUpgpNh": "co_infection_type",  # MVE - LAB - Si Co-infection
    "Smg0g56IqWr": "incident_labo",  # MVE - LAB - Incident
    # Stage 5 · Statut final du patient (kOyiPgabAuY)
    "Za0cx3pmcWW": "statut_final_patient",  # 199 - MVE - S6 - Statut final du patient
    "jieNzfUp3E8": "signes_hemorragiques_maladie",  # 200 - MVE - S6 - Est-ce-que le patient a eu des signes hémorragiques inexpliqués pendant la durée de la maladie?
    "W2u38gg9Jy8": "date_sortie_isolement",  # 205 - MVE - S6 - Si le malade était en isolement, date de sortie de la zone d’isolement  # noqa: RUF003
    "wIY8Kv2oWec": "date_sortie_hopital",  # 206 - MVE - S6 - Date de sortie de l’hôpital  # noqa: RUF003
    "x1aazi4fgKO": "date_deces_final",  # 208 - MVE - S6 - Date de décès — cf. note plus haut (≠ date_deces reconstruite de la table)
    "sHEARVNufMJ": "lieu_deces",  # 209 - MVE - S6 - Lieu du décès
    "fg5xfl9bD5V": "zone_sante_guerison",  # 204 - MVE - S6 - SG - Zone de santé
    "dqmYvLDGfDu": "zone_sante_deces",  # 212 - MVE - S6 - DC - Zone de Santé
    "eLqoRcK7lq1": "date_funerailles",  # 214 - MVE - S6 - Date des funérailles
    "fpw6gIG7Nhq": "funerailles_organisees_par",  # 215 - MVE - S6 - Funérailles organisées par
    "LE2eGGkAy2F": "lieu_funerailles",  # 216 - MVE - S6 - Lieu des funérailles/enterrement
    "NympO1c3msQ": "zone_sante_funerailles",  # 218 - MVE - S6 - FE - Zone de Santé
    # Stage 6 · Prise en charge (rMvKKqab4bW)
    "Xy5J5MGpaZ7": "date_sortie_cte",  # MVE - PEC - Date de sortie
    "KGsTJ4jV7Fb": "date_admission_cte",  # MVE - PEC - Date d’admission  # noqa: RUF003
    "AawdHKqaXcj": "statut_avant_admission_cte",  # MVE - PEC - Status avant admission
    "WKZu0kp6wWu": "modalite_sortie_cte",  # MEV - PEC37 - Statut au moment de la sortie
    "eFA1FPYnvsj": "etablissement_pec_actuel",  # MVE - PEC04 - Nom de l'etablissement de soins (CTE, ESS) : Actuel
    "CJaBDAUQ0kr": "provenance_patient_pec",  # MVE - PEC01 - Provenance du patient
    "wcPpsQhBslQ": "vacinnation_ebola_pec",  # MEV - PEC11.3 - Vaccination ebola
    "x0eZQyz12IZ": "date_vaccination_pec",  # MEV - PEC11.4 - Date de vaccination
    # Stage 7 · Prévention et Contrôle des Infections (RMwCRkCgqlv)
    "lgy9pGUinCP": "isolement_pci",  # MVE - PCI1 - Isolement
    "x3hPEw1dqqZ": "date_isolement_pci",  # MVE - PCI2 - Date d’isolement  # noqa: RUF003
    "cPYPUNYgWbc": "lieu_isolement_pci",  # MVE - PCI3 - Lieu Isolement
    "CFyW6yNLQXU": "decontamination_pci",  # MVE - PCI4 - Décontamination
    "OW2wPtfKUcL": "dotation_kits_pci_ess_pci",  # MVE - PCI7 - Dotation de Kits PCI ESS
    "jy0OuCPDBli": "nombre_kits_pci_ess_pci",  # MVE - PCI8 - Nombre de Kits PCI ESS
    "fWBGDpJezOX": "date_deces_pci",  # MVE - PCI9 - Date de décès
    "plhgmEjNwl5": "date_intervention_activite_pci",  # MVE - PCI-001 - Date de l'intervention/activité
    "w87DUjQZkw1": "etablissement_soins_sante_pci",  # MVE - PCI-005 - Nom de l'Etablissement de soins de santé
    "XRhlecJPQbg": "nombre_total_lits_pci",  # MVE - PCI-008 - Nombre total de lits
    "uWaXCTdBlxC": "date_investigation_ppl_pci",  # MVE - PCI-018 - Date d'investigation du PPL
    "oZalwEvTdSi": "eds_realise_pci",  # MVE - PCI12 - EDS realise
    "jxdmebLuEKC": "date_eds_pci",  # MVE - PCI11 - Date EDS
    "WTtRj0ODuBE": "swab_realise_pci",  # MVE - PCI10 - Swab realise
    "b6Pf3aEdtpP": "date_swab_pci",  # MVE - PCI11 - Date Swab
    # Stage 8 · Localisation du cas confirmé (U7LGPqXkVg6)
    "B06dikdGoC5": "gps_cas_confirme",  # MVE - Coordonnées géographiques — Cas confirmé
}

DE_UTILES = sorted({*DATASET_LLN_MAPPING, *DICO_DE_MAPPING.values()})


LLN_EXPORT_DIR = "pipelines/lln"
LLN_DATASET_FILE = "lln_mve_notifications.parquet"
LLN_DATASET_META = "lln_mve_notifications_metadata.json"

# Canonisation géographique des libellés DHIS2 (« it Ituri Province » -> « Ituri »)
GEO_PREFIX_RE = r"^[a-z]{2}\s+"
GEO_SUFFIX_RE = r"(?i)\s+(Province|Zone de Santé|Zone_de_sante|Aire de Santé)$"
PROVINCE_CANONICAL = {
    "Nord Kivu": "Nord-Kivu",
    "Sud Kivu": "Sud-Kivu",
    "Haut Uele": "Haut-Uele",
    "Bas Uele": "Bas-Uele",
}

# Clés d'identification (grain publié = un enrôlement par ligne)
COLS_LLN_CLES = ["enrollment_id", "tracked_entity_id"]

# Géographie canonisée (les level_*_name bruts DHIS2 ne sont pas publiés)
COLS_LLN_GEO = [
    "province_id",
    "province",
    "zone_sante_id",
    "zone_sante",
    "aire_sante_id",
    "aire_sante",
]

# Attributs d'entité suivie + fenêtre de suivi de l'enrôlement
COLS_LLN_TEI = [
    "numero_epid",
    "sexe",
    "age_ans",
    "age_mois",
    "profession",
    "secteur",
    "lien_epidemiologique",
    "date_notification",
    "date_debut_symptomes",
]
COLS_LLN_EVENTS = ["enrolled_at", "date_premier_event", "date_dernier_event", "n_events"]

# Résumé de l'historique laboratoire (cf. build_lab_summary)
COLS_LLN_LABO = [
    "lab_confirme",
    "date_confirmation",
    "date_dernier_test",
    "n_tests_labo",
    "n_pos",
    "n_neg",
    "n_inv",
    "flag_pos_puis_neg",
    "flag_pos_puis_inv",
]

# Drapeaux is_* et date de décès reconstruite (cf. compute_lln_flags), alignés
# sur la méthodologie de COD_MVE_Tracker_Individu (compute_indicators /
# reconstruct_date_deces dans pipeline.py). Ajoutés en fin de schéma : aucun de
# ces noms ne recouvre une colonne déjà publiée plus haut (LLN_MAPPING, TEI,
# labo…), donc pas de doublon de nom dans le dataset.
COLS_LLN_FLAGS = [
    "is_alerte",
    "is_alerte_valide",
    "is_suspect",
    "is_confirme",
    "is_preleve",
    "is_recu",
    "is_analyse",
    "is_resultat_valide",
    "is_deces",
    "is_gueri",
    "is_deces_confirme",
    "is_confirme_gueri",
    "is_confirme_vivant",
    "date_deces",
]

_DATASET_LLN_BLOCS = [
    COLS_LLN_CLES,
    COLS_LLN_GEO,
    COLS_LLN_TEI,
    COLS_LLN_EVENTS,
    list(DATASET_LLN_MAPPING.values()),
    COLS_LLN_LABO,
    COLS_LLN_FLAGS,
]

# Un même nom de colonne publié par deux blocs romprait silencieusement le
# schéma (la seconde occurrence écraserait la première) : on l'échoue au
# chargement du module plutôt que de laisser `dict.fromkeys` dédoublonner en
# silence.
_toutes_colonnes = [col for bloc in _DATASET_LLN_BLOCS for col in bloc]
_doublons = {col for col in _toutes_colonnes if _toutes_colonnes.count(col) > 1}
if _doublons:
    raise ValueError(f"DATASET_LLN_COLS : nom(s) de colonne dupliqué(s) entre blocs : {_doublons}")

DATASET_LLN_COLS = list(dict.fromkeys(_toutes_colonnes))
