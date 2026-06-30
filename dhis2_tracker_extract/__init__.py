"""Pipeline OpenHexa d'extraction des trackers MVE (DHIS2).

Extrait les événements des deux programmes tracker (notification/investigation
et fiche contacts), les enrichit (enrôlements, tracked entities, métadonnées des
data elements, hiérarchie géographique) et décode les valeurs codées dans une
colonne ``value_norm``. Produit une table « longue » dédiée par tracker, prête
pour l'agrégation et le calcul des indicateurs du SitRep.
"""
