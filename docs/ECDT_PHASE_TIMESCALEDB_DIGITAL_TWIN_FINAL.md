# ECDT — Étape 4 : Digital Twin temporel avec TimescaleDB

## 1. Présentation

Cette étape a pour objectif de compléter le Knowledge Graph de l'Enterprise Cognitive Digital Twin (ECDT) par une représentation temporelle de l'état de l'infrastructure.

Le principe architectural retenu est :

```text
                    ECDT
                     |
          +----------+----------+
          |                     |
          v                     v
    Knowledge Graph       Digital Twin temporel
        Neo4j                 TimescaleDB
          |                     |
          |                     |
   structure / topology     metrics / history
          |                     |
          +----------+----------+
                     |
              resource_id
                     |
                     v
          Future temporal RCA
```

Le Knowledge Graph représente principalement les ressources et leurs dépendances, tandis que TimescaleDB représente l'évolution temporelle des métriques associées aux ressources.

Cette séparation correspond à l'architecture définie dans le cadrage technique du projet : Neo4j répond à la question « qui dépend de qui ? », tandis que TimescaleDB répond à la question « que s'est-il passé, et quand ? ».

---

# 2. Objectifs

Les objectifs de l'étape étaient :

- [x] Installer et configurer TimescaleDB.
- [x] Vérifier PostgreSQL et l'extension TimescaleDB.
- [x] Définir le schéma de stockage des observations métriques.
- [x] Transformer les métriques normalisées de la Phase 2 vers le format attendu.
- [x] Développer l'ingestion vers TimescaleDB.
- [x] Permettre la recherche d'un historique par ressource.
- [x] Permettre la recherche de métriques autour d'un timestamp.
- [x] Valider l'intégration avec de vraies données de la Phase 2.
- [x] Nettoyer les données artificielles et les doublons introduits pendant les validations.

## Livrable

> Historique de métriques interrogeable par ressource et par période, distinct du Knowledge Graph mais relié à celui-ci via l'identifiant de ressource.

Le livrable est fonctionnellement atteint.

---

# 3. Positionnement dans l'architecture ECDT

La chaîne de traitement validée est :

```text
RCAEval
   |
   v
Phase 1
Dataset / Ground Truth / Topology
   |
   v
Phase 2
Ingestion / Normalisation / Anomaly Detection
   |
   +----------------------+
   |                      |
   v                      v
Knowledge Graph       Digital Twin
Neo4j                 TimescaleDB
   |                      |
   | topology             | temporal metrics
   | dependencies         | resource history
   |                      | time windows
   +----------+-----------+
              |
              v
        Future RCA
              |
              v
     Temporal + Structural
         Correlation
```

Cette organisation est cohérente avec le cadrage technique du projet, qui prévoit trois briques complémentaires : Knowledge Graph, Digital Twin Time Series et couche cognitive multi-agents.

---

# 4. Technologie utilisée

## PostgreSQL

La base TimescaleDB utilisée dans Docker repose sur :

```text
PostgreSQL 16.15
```

## TimescaleDB

Extension installée et vérifiée :

```text
TimescaleDB 2.29.2
```

La commande de vérification utilisée est :

```powershell
docker compose exec timescaledb psql -U ecdt -d ecdt -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';"
```

Résultat final :

```text
timescaledb | 2.29.2
```

---

# 5. Configuration Docker

Le service utilisé est :

```yaml
timescaledb:
    image: timescale/timescaledb:latest-pg16
```

Configuration validée :

```text
Database : ecdt
User     : ecdt
Port     : 5432
```

Le backend utilise une URI TimescaleDB correspondant à cette configuration.

La configuration Docker a été corrigée pendant la validation afin que :

```text
Docker Compose
       |
       v
TimescaleDB
       |
       v
PostgreSQL / ecdt
```

utilise bien le même compte et la même base que le client Python.

---

# 6. Schéma temporel

La table principale est :

```text
metric_observations
```

Elle est configurée comme hypertable TimescaleDB.

La vérification finale :

```powershell
docker compose exec timescaledb psql -U ecdt -d ecdt -c "SELECT hypertable_schema, hypertable_name FROM timescaledb_information.hypertables;"
```

a retourné :

```text
public | metric_observations
```

## Modèle logique

Une observation contient notamment :

```text
resource_id
timestamp
value
metric_type
metric_name
case_id
dataset
fault
```

Le noyau minimal nécessaire au Digital Twin temporel est donc :

```text
resource_id
timestamp
value
metric_type
```

avec les informations de contexte de cas disponibles en complément.

---

# 7. Identifiant de ressource

Le champ :

```text
resource_id
```

est le point de liaison entre la représentation temporelle et la représentation structurelle.

Exemple :

```text
Neo4j
Service
  id = checkoutservice
        |
        | resource_id
        v
TimescaleDB
metric_observations
  resource_id = checkoutservice
```

Cela permet aux futures fonctions RCA de combiner :

```text
Knowledge Graph
       +
TimescaleDB
```

sans fusionner les deux systèmes de stockage.

---

# 8. Client TimescaleDB

Le client Python est situé dans :

```text
src/digital_twin/timescale_client.py
```

Il fournit la couche d'accès à TimescaleDB utilisée par les autres composants.

La connexion a été validée avec :

```text
PASS: TimescaleDB connection
```

La validation directe PostgreSQL a également confirmé :

```text
current_user     = ecdt
current_database = ecdt
```

---

# 9. Schéma TimescaleDB

La logique de création/initialisation du schéma est portée par :

```text
src/digital_twin/timeseries_schema.py
```

La validation finale a confirmé :

```text
PASS: TimescaleDB schema
```

et :

```text
metric_observations
```

est bien une hypertable TimescaleDB.

---

# 10. Ingestion

L'ingestion est portée par :

```text
src/digital_twin/timeseries_ingestion.py
```

La chaîne utilisée pendant la validation réelle est :

```text
Phase 2 metrics
      |
      v
DatasetLoader
      |
      v
Polars DataFrame
      |
      v
SchemaNormalizer
      |
      v
Normalized metrics
      |
      v
Timescale ingestion
      |
      v
metric_observations
```

---

# 11. Intégration avec la Phase 2

La validation finale n'a pas utilisé uniquement une donnée artificielle.

Le cas réel utilisé est :

```text
re2ob_checkoutservice_cpu_1
```

Le loader Phase 2 a chargé :

```text
785 345
```

lignes de métriques.

Commande conceptuelle utilisée :

```python
loader.load_metrics(
    case_id="re2ob_checkoutservice_cpu_1",
    long_format=True,
)
```

Résultat :

```text
Metrics rows loaded: 785345
PASS: real Phase 2 metrics loaded
```

---

# 12. Normalisation réelle

Le `SchemaNormalizer` de la Phase 2 a ensuite produit :

```text
785 345
```

lignes normalisées.

Le schéma canonique validé contient 19 champs :

```text
event_id
case_id
timestamp_ms
dataset
fault
root_cause_service
source
service_name
signal_type
metric_name
value
message
trace_id
span_id
parent_span_id
method_name
operation_name
duration_ms
status_code
```

Résultat :

```text
PASS: metric normalization
PASS: normalized schema contains required fields
```

Cela confirme que TimescaleDB reçoit bien des données issues du modèle normalisé de la Phase 2.

---

# 13. Sélection d'une métrique réelle

Pour la validation d'intégration, une métrique réelle correspondant à :

```text
case_id      = re2ob_checkoutservice_cpu_1
service_name = checkoutservice
signal_type  = cpu
```

a été sélectionnée.

Le test a trouvé :

```text
1 441
```

lignes correspondant à `checkoutservice` + `cpu`.

Une observation réelle a été utilisée :

```text
case_id       = re2ob_checkoutservice_cpu_1
service_name  = checkoutservice
signal_type   = cpu
metric_name   = checkoutservice_cpu
timestamp_ms  = 1705353846000
value         = 0.21588648332356936
```

---

# 14. Ingestion réelle

L'observation a ensuite été envoyée à TimescaleDB.

Résultat :

```text
Inserted observations: 1
PASS: real metric inserted into TimescaleDB
```

Cela constitue la validation essentielle de l'intégration :

```text
Phase 2
   |
   v
NormalizedEvent
   |
   v
Timescale ingestion
   |
   v
TimescaleDB
```

---

# 15. Requête par ressource

La fonction de requête d'historique permet de récupérer les observations associées à une ressource.

Exemple :

```python
get_resource_history(
    client,
    resource_id="checkoutservice",
)
```

Résultat de validation :

```text
Rows returned: 3
PASS: resource history query
```

Après nettoyage des données de test, le cas réel utilisé pour la validation possède une observation :

```text
checkoutservice
2024-01-15 21:24:06+00
0.21588648332356936
cpu
checkoutservice_cpu
re2ob_checkoutservice_cpu_1
```

---

# 16. Requête temporelle

Une deuxième fonctionnalité importante permet de retrouver les métriques autour d'un timestamp.

Le principe est :

```text
                timestamp
                    |
          +---------+---------+
          |                   |
       -window              +window
          |                   |
          +---------+---------+
                    |
                    v
             observations
```

Cette fonctionnalité est destinée aux futures opérations de corrélation temporelle.

La validation précédente a confirmé :

```text
PASS: temporal window query
```

avec une observation réelle retrouvée autour du timestamp demandé.

---

# 17. Validation artificielle

Avant l'intégration avec les vraies données, une observation artificielle avait été utilisée :

```text
case_id      = validation_case_001
resource_id  = checkoutservice
metric_type  = cpu
metric_name  = checkoutservice_cpu
value        = 73.5
```

Cette donnée a servi à valider :

- la connexion ;
- le schéma ;
- l'insertion ;
- la recherche par ressource ;
- la recherche temporelle.

Elle a ensuite été supprimée.

Vérification finale :

```text
validation_case_001
```

n'est plus présente dans la table.

---

# 18. Nettoyage des doublons

Pendant les validations d'intégration, la même observation réelle avait été insérée deux fois.

Les deux lignes étaient identiques :

```text
resource_id  = checkoutservice
timestamp    = 2024-01-15 21:24:06+00
value        = 0.21588648332356936
metric_type  = cpu
metric_name  = checkoutservice_cpu
case_id      = re2ob_checkoutservice_cpu_1
```

Un nettoyage ciblé a été effectué.

La vérification finale :

```powershell
docker compose exec timescaledb psql -U ecdt -d ecdt -c "SELECT case_id, COUNT(*) AS observations FROM metric_observations GROUP BY case_id ORDER BY case_id;"
```

a retourné :

```text
re2ob_checkoutservice_cpu_1 | 1
```

La base de validation est donc propre.

---

# 19. Backup de validation

Avant le nettoyage, une sauvegarde de la table a été créée :

```powershell
docker compose exec timescaledb pg_dump -U ecdt -d ecdt -t metric_observations > metric_observations_backup.sql
```

Ce fichier correspond au backup de la table au moment de la validation.

---

# 20. Tests réalisés

Le test principal a été exécuté avec :

```powershell
python -m tests.test_timescale_connection
```

Le résultat final est :

```text
=== FINAL TIMESCALEDB INTEGRATION VALIDATION ===

[1] TimescaleDB connection
PASS: TimescaleDB connection
PASS: TimescaleDB schema

[2] Loading real Phase 2 metrics
Metrics rows loaded: 785345
PASS: real Phase 2 metrics loaded

[3] Normalizing real metric data
Normalized rows: 785345
PASS: metric normalization

[4] Inspecting normalized data
PASS: normalized schema contains required fields

[5] Selecting real checkoutservice CPU metric
Matching normalized rows: 1441
PASS: real checkoutservice CPU metric found

[6] Preparing TimescaleDB ingestion event
PASS: ingestion event prepared

[7] Ingesting real Phase 2 metric
Inserted observations: 1
PASS: real metric inserted into TimescaleDB

[8] Querying checkoutservice history
PASS: resource history query

[9] Verifying persisted Phase 2 observation
PASS: persisted Phase 2 metric

ALL FINAL TIMESCALEDB INTEGRATION VALIDATIONS PASSED
```

---

# 21. Vérifications PostgreSQL / TimescaleDB

## Version PostgreSQL

```text
PostgreSQL 16.15
```

## Version TimescaleDB

```text
2.29.2
```

## Hypertable

```text
public.metric_observations
```

## Utilisateur

```text
ecdt
```

## Base

```text
ecdt
```

Toutes ces vérifications ont été effectuées avec PostgreSQL exécuté dans Docker Compose.

---

# 22. État final de l'étape

```text
PHASE — DIGITAL TWIN / TIMESCALEDB
===================================

[x] TimescaleDB installé
[x] PostgreSQL configuré
[x] Extension TimescaleDB active
[x] Connexion Python → TimescaleDB
[x] Schéma métrique
[x] Hypertable metric_observations
[x] resource_id
[x] timestamp
[x] value
[x] metric_type
[x] metric_name
[x] Ingestion des métriques normalisées
[x] Intégration réelle avec Phase 2
[x] Historique par ressource
[x] Requête temporelle autour d'un timestamp
[x] Nettoyage des données de validation
[x] Vérification finale de la base

STATUS: COMPLETED
```

---

# 23. Limites et remarques

## 23.1 Ce qui est validé

La couche TimescaleDB est fonctionnelle pour :

```text
stockage temporel
+
ingestion
+
recherche par ressource
+
recherche par période
```

## 23.2 Ce qui n'est pas encore réalisé

Cette étape ne réalise pas encore :

```text
corrélation automatique multi-sources
ranking de root causes
raisonnement causal
RCA complète
orchestration multi-agents
corrélation Neo4j + TimescaleDB automatisée
```

Ces fonctions appartiennent aux étapes suivantes du projet.

## 23.3 Métadonnées dataset/fault

Le schéma TimescaleDB prévoit des champs de contexte tels que :

```text
dataset
fault
```

Cependant, le test final d'ingestion a volontairement construit l'événement minimal à partir de :

```text
case_id
timestamp_ms
service_name
signal_type
metric_name
value
```

Les lignes insérées lors de cette validation finale ont donc `dataset` et `fault` à `NULL`.

Cela ne bloque pas le livrable de cette étape, dont le noyau est l'historique métrique par ressource et période. La propagation complète de toutes les métadonnées normalisées pourra être renforcée lors de l'intégration générale du pipeline.

---

# 24. Relation avec le Knowledge Graph

Les deux bases ont des responsabilités distinctes.

## Neo4j

```text
Service
   |
   +-- DEPENDS_ON --> Service
   |
   +-- Incident
   |
   +-- CAUSED_BY --> Service
```

Neo4j répond principalement à :

```text
Qui dépend de qui ?
Quels services sont en amont ?
Quels services sont en aval ?
Quelle est la root cause connue ?
```

## TimescaleDB

```text
resource_id
timestamp
metric_type
metric_name
value
```

TimescaleDB répond principalement à :

```text
Quelle était la valeur d'une métrique ?
À quel moment ?
Sur quelle ressource ?
Comment le signal évolue-t-il avant et après un incident ?
```

La future RCA pourra combiner les deux :

```text
             Incident
                 |
       +---------+---------+
       |                   |
       v                   v
   Neo4j               TimescaleDB
       |                   |
 dependencies          temporal signals
       |                   |
       +---------+---------+
                 |
                 v
          Temporal + Structural
             Correlation
```

---

# 25. Transition vers la suite

Les étapes précédentes ont maintenant fourni :

```text
Phase 1
Dataset / Ground Truth / Topology
        |
        v
Phase 2
Normalized Events / Anomalies
        |
        +----------------------+
        |                      |
        v                      v
Phase 3                  Digital Twin
Knowledge Graph           TimescaleDB
Neo4j                     Time Series
        |                      |
        +----------+-----------+
                   |
                   v
             Future RCA
```

La plateforme dispose donc désormais des deux représentations fondamentales du Digital Twin :

```text
STRUCTURE
   -> Neo4j

ÉTAT TEMPOREL
   -> TimescaleDB
```

La prochaine couche pourra exploiter simultanément ces deux représentations pour construire la corrélation nécessaire à la Root Cause Analysis.

---

# 26. Conclusion

Cette étape a transformé les métriques normalisées de la Phase 2 en une représentation temporelle persistante et interrogeable.

La validation finale a démontré que :

```text
Phase 2
785 345 métriques
       |
       v
SchemaNormalizer
       |
       v
Normalized metrics
       |
       v
TimescaleDB
       |
       v
resource history
       |
       v
temporal queries
```

fonctionne de bout en bout sur des données réelles.

Le livrable demandé est donc atteint :

> **historique de métriques interrogeable par ressource et par période, distinct du Knowledge Graph mais relié à celui-ci via l'identifiant de ressource.**

## Statut final

```text
DIGITAL TWIN — TIMESCALEDB
STATUS: COMPLETED
```

---

# 27. Références internes du projet

Documentation et sources utilisées :

```text
docs/cadrage_technique.md

docs/ECDT_PHASE_1_RCAEval_Documentation.md

docs/ECDT_PHASE_2_Ingestion_Normalization_Anomaly_Detection.md

docs/ECDT_PHASE_3_KNOWLEDGE_GRAPH.md

src/ingestion/dataset_loader.py

src/ingestion/schema_normalizer.py

src/digital_twin/

tests/test_timescale_connection.py
```

Le cadrage technique décrit TimescaleDB comme la couche Digital Twin Time Series complémentaire au Knowledge Graph Neo4j. Les documents des phases précédentes établissent la continuité entre les données RCAEval, leur normalisation et les représentations structurelles et temporelles d'ECDT.
