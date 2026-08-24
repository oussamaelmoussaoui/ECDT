# ECDT — Phase 3 : Knowledge Graph

## 1. Présentation

Cette étape a pour objectif de représenter l’infrastructure et ses dépendances sous la forme d’un **Knowledge Graph Neo4j** exploitable par la plateforme ECDT.

Le graphe constitue la représentation structurée de l’infrastructure nécessaire aux futures fonctions de diagnostic, de propagation d’impact et de Root Cause Analysis (RCA).

La construction du graphe s’appuie sur les données validées lors des étapes précédentes, en particulier :

- la topologie extraite et normalisée à l’Étape 1 ;
- le ground truth des incidents RCAEval ;
- les services et dépendances stockés dans `data/processed/topology/` ;
- les incidents stockés dans `data/ground_truth/target_incidents.csv`.

---

## 2. Objectifs de l’étape

Les objectifs définis pour cette étape étaient :

- [x] Installer et configurer Neo4j.
- [x] Définir le schéma du graphe.
- [x] Développer le mécanisme de peuplement du graphe.
- [x] Écrire les requêtes Cypher principales.
- [x] Tester les fonctionnalités du Knowledge Graph.
- [x] Vérifier l’intégration avec la suite de tests du projet.

### Livrable

> **Knowledge Graph Neo4j peuplé et interrogeable, représentant la topologie et les relations d’incidents du dataset ECDT.**

---

# 3. Architecture de la solution

La partie Knowledge Graph est organisée ainsi :

```text
src/
└── knowledge_graph/
    ├── __init__.py
    ├── neo4j_client.py
    ├── graph_schema.py
    ├── graph_builder.py
    └── graph_queries.py
```

Les tests sont regroupés dans :

```text
tests/
└── test_knowledge_graph.py
```

Les données utilisées sont :

```text
data/
├── ground_truth/
│   └── target_incidents.csv
│
└── processed/
    └── topology/
        ├── services.csv
        ├── dependencies.csv
        └── re2ob_checkoutservice_delay_topology.csv
```

---

# 4. Neo4j

## 4.1. Rôle de Neo4j

Neo4j est utilisé comme moteur de base de données orienté graphe.

Dans ECDT, il permet de représenter :

- les services ;
- les incidents ;
- les dépendances entre services ;
- les relations entre incidents et leurs root causes.

Cette représentation est particulièrement adaptée aux futures opérations de diagnostic et d’analyse d’impact.

---

## 4.2. Déploiement

Neo4j est exécuté avec Docker.

Le démarrage du service est effectué depuis la racine du projet :

```powershell
docker compose up -d neo4j
```

La disponibilité du conteneur peut être vérifiée avec :

```powershell
docker ps
```

La connexion Neo4j utilisée par l’application a été validée avant la construction du graphe.

---

# 5. Client Neo4j

Le fichier :

```text
src/knowledge_graph/neo4j_client.py
```

centralise la communication entre Python et Neo4j.

Il est responsable notamment de :

- créer la connexion ;
- vérifier la connectivité ;
- exécuter les requêtes Cypher ;
- gérer l’ouverture et la fermeture de la connexion.

La séparation du client permet aux autres composants du Knowledge Graph de ne pas gérer directement les détails de connexion.

---

# 6. Schéma du Knowledge Graph

Le schéma défini pour ECDT prévoit les types de nœuds suivants :

```text
Service
Pod
Database
Node
Incident
```

et les relations :

```text
DEPENDS_ON
RUNS_ON
IMPACTS
CAUSED_BY
```

## 6.1. Modèle actuellement peuplé

Les données validées disponibles pour cette étape permettent directement de peupler :

```text
(:Service)
(:Incident)
```

avec :

```text
(:Service)-[:DEPENDS_ON]->(:Service)

(:Incident)-[:CAUSED_BY]->(:Service)
```

Les nœuds `Pod`, `Database` et `Node`, ainsi que les relations `RUNS_ON` et `IMPACTS`, restent prévus par le modèle mais ne sont pas artificiellement déduits lorsqu’une source validée ne les fournit pas.

Cette décision permet de conserver la traçabilité des données et d’éviter d’inventer des relations d’infrastructure.

---

# 7. Convention des relations

## 7.1. DEPENDS_ON

La relation :

```cypher
(:Service)-[:DEPENDS_ON]->(:Service)
```

représente une dépendance entre deux services.

La convention utilisée est :

```text
source_service -> target_service
```

Exemple :

```text
frontendservice
       |
       | DEPENDS_ON
       v
checkoutservice
```

Cette convention correspond à la topologie construite lors de l’étape précédente.

---

## 7.2. CAUSED_BY

La relation :

```cypher
(:Incident)-[:CAUSED_BY]->(:Service)
```

représente la root cause connue dans le ground truth.

Exemple :

```text
re2ob_checkoutservice_cpu_1
              |
              | CAUSED_BY
              v
       checkoutservice
```

Cette relation permet de conserver la vérité terrain dans le Knowledge Graph.

Elle pourra ensuite être utilisée pour comparer les résultats d’un futur agent RCA aux root causes connues.

---

# 8. Construction du graphe

Le fichier :

```text
src/knowledge_graph/graph_builder.py
```

est responsable du peuplement de Neo4j.

Il réalise les opérations suivantes :

```text
1. Charger services.csv
        |
        v
2. Charger dependencies.csv
        |
        v
3. Charger target_incidents.csv
        |
        v
4. Valider les données
        |
        v
5. Créer les Service
        |
        v
6. Créer les DEPENDS_ON
        |
        v
7. Créer les Incident
        |
        v
8. Créer les CAUSED_BY
        |
        v
9. Vérifier le graphe
```

---

# 9. Sources de données

## 9.1. Services

Source :

```text
data/processed/topology/services.csv
```

La topologie validée contient :

```text
34 services
```

Ces services correspondent aux services observés dans la topologie extraite.

---

## 9.2. Dépendances

Source :

```text
data/processed/topology/dependencies.csv
```

La topologie validée contient :

```text
64 dépendances
```

Ces relations sont représentées par :

```text
(:Service)-[:DEPENDS_ON]->(:Service)
```

---

## 9.3. Ground truth

Source :

```text
data/ground_truth/target_incidents.csv
```

Le ground truth utilisé pour le Knowledge Graph contient :

```text
60 incidents
```

Chaque incident peut être associé à un `root_cause_service`.

---

# 10. Gestion des services du ground truth

Une particularité importante a été identifiée lors de la construction du graphe.

Certains services présents dans le ground truth n’étaient pas présents dans la topologie des 34 services.

Les services concernés sont :

```text
carts
catalogue
orders
payment
user
```

Ils sont donc créés comme nœuds `Service` afin de permettre la représentation correcte des relations `CAUSED_BY`.

Cependant, aucune dépendance topologique n’est inventée pour ces services lorsqu’elle n’est pas présente dans `dependencies.csv`.

Le graphe final contient ainsi :

```text
34 services provenant de la topologie
+
5 services présents uniquement dans le ground truth
=
39 Service
```

Cette distinction préserve la provenance des informations.

---

# 11. Idempotence du peuplement

Le builder utilise `MERGE` pour la création des nœuds et relations.

Exemple :

```cypher
MERGE (s:Service {id: $service})
```

et :

```cypher
MERGE (source)-[:DEPENDS_ON]->(target)
```

Cela permet de relancer le builder sans créer de doublons.

Le peuplement est donc conçu pour être idempotent.

---

# 12. Exécution du builder

Depuis la racine du projet :

```powershell
python -m src.knowledge_graph.graph_builder
```

Le builder charge les données, valide les entrées, peuple Neo4j et vérifie les statistiques du graphe.

Le résultat attendu après peuplement est :

```text
39 Service
60 Incident
64 DEPENDS_ON
60 CAUSED_BY
```

Soit :

```text
99 nœuds
124 relations
```

---

# 13. Requêtes du Knowledge Graph

Le fichier :

```text
src/knowledge_graph/graph_queries.py
```

centralise les requêtes Cypher en lecture.

Les principales fonctions sont :

```text
get_service()
get_direct_dependencies()
get_direct_dependents()
get_upstream_dependencies()
get_downstream_impacts()
get_incident_root_cause()
get_service_incidents()
get_incident()
get_service_neighborhood()
get_graph_statistics()
get_graph_overview()
```

---

# 14. Dépendances directes

Pour obtenir les dépendances directes d’un service :

```cypher
MATCH
    (source:Service {id: $service_id})
    -[:DEPENDS_ON]->
    (target:Service)

RETURN
    target.id AS service

ORDER BY service
```

Cette requête suit la direction :

```text
Service
   |
   | DEPENDS_ON
   v
Service dépendant
```

---

# 15. Dépendances amont

Pour remonter les dépendances amont :

```cypher
MATCH (target:Service {id: $service_id})

MATCH path =
    (upstream:Service)
    <-[:DEPENDS_ON*1..5]-
    (target)

WITH
    upstream,
    min(length(path)) AS depth

RETURN
    upstream.id AS service,
    depth

ORDER BY depth, service
```

Cette fonctionnalité est importante pour l’analyse RCA.

Elle permet de partir d’un service affecté et d’explorer les services situés en amont dans le graphe.

---

# 16. Impacts aval

Pour rechercher les impacts aval :

```cypher
MATCH (source:Service {id: $service_id})

MATCH path =
    (source)
    -[:DEPENDS_ON*1..5]->
    (downstream:Service)

WITH
    downstream,
    min(length(path)) AS depth

RETURN
    downstream.id AS service,
    depth

ORDER BY depth, service
```

Cela permet d’identifier les services potentiellement affectés lorsqu’un service donné rencontre un problème.

---

# 17. Root cause d’un incident

Pour retrouver la root cause d’un incident :

```cypher
MATCH
    (i:Incident {id: $incident_id})
    -[:CAUSED_BY]->
    (s:Service)

RETURN
    i.id AS incident,
    s.id AS root_cause
```

Exemple de validation :

```text
re2ob_checkoutservice_cpu_1
            |
        CAUSED_BY
            |
            v
    checkoutservice
```

---

# 18. Incidents associés à un service

La requête suivante permet de rechercher les incidents dont un service est la root cause :

```cypher
MATCH
    (i:Incident)
    -[:CAUSED_BY]->
    (s:Service {id: $service_id})

RETURN
    i.id AS incident,
    i.dataset AS dataset,
    i.fault AS fault
ORDER BY incident
```

Cette fonctionnalité sera utile pour la future mémoire des incidents et la recherche de cas similaires.

---

# 19. Voisinage d’un service

Le voisinage direct d’un service peut être récupéré avec :

```cypher
MATCH (s:Service {id: $service_id})

OPTIONAL MATCH
    (s)-[:DEPENDS_ON]->(dependency:Service)

OPTIONAL MATCH
    (dependent:Service)-[:DEPENDS_ON]->(s)

RETURN
    s.id AS service,
    collect(DISTINCT dependency.id) AS dependencies,
    collect(DISTINCT dependent.id) AS dependents
```

Le résultat distingue :

```text
dependencies
```

et :

```text
dependents
```

---

# 20. Statistiques du graphe

Les statistiques principales sont obtenues avec :

```cypher
MATCH (s:Service)
RETURN count(s)
```

```cypher
MATCH (i:Incident)
RETURN count(i)
```

```cypher
MATCH (:Service)-[r:DEPENDS_ON]->(:Service)
RETURN count(r)
```

```cypher
MATCH (:Incident)-[r:CAUSED_BY]->(:Service)
RETURN count(r)
```

Les valeurs attendues sont :

| Élément | Nombre |
|---|---:|
| Service | 39 |
| Incident | 60 |
| `DEPENDS_ON` | 64 |
| `CAUSED_BY` | 60 |

---

# 21. Tests automatisés

Le fichier :

```text
tests/test_knowledge_graph.py
```

contient les tests du Knowledge Graph.

Les tests couvrent notamment :

- connexion Neo4j ;
- statistiques du graphe ;
- recherche de services ;
- gestion des services inexistants ;
- dépendances directes ;
- dépendants directs ;
- dépendances amont ;
- impacts aval ;
- validation de `max_depth` ;
- root cause des incidents ;
- recherche d’incidents ;
- voisinage des services ;
- cohérence des relations ;
- absence de relations cassées ;
- comportement en lecture seule.

---

# 22. Validation de la cohérence

Les tests vérifient notamment que :

### Chaque incident possède une root cause

```text
60 Incident
60 CAUSED_BY
```

avec une relation `CAUSED_BY` par incident.

### Les dépendances sont valides

Chaque relation :

```text
(:Service)-[:DEPENDS_ON]->(:Service)
```

doit relier deux nœuds `Service`.

### Les relations CAUSED_BY sont valides

Chaque relation :

```text
(:Incident)-[:CAUSED_BY]->(:Service)
```

doit relier un `Incident` à un `Service`.

### Les requêtes de lecture ne modifient pas le graphe

Les statistiques avant et après l'exécution des requêtes doivent rester identiques.

---

# 23. Validation finale

La suite complète des tests du projet a été exécutée avec :

```powershell
python -m pytest tests -v
```

Résultat final :

```text
49 passed in 319.47s (0:05:19)
```

Cette exécution confirme que les tests de l'ensemble du projet passent après l'intégration du Knowledge Graph.

---

# 24. État final de l'étape

```text
PHASE 3 — KNOWLEDGE GRAPH
==========================

[x] Neo4j installé/configuré
[x] Connexion Python → Neo4j
[x] Schéma du graphe
[x] Services
[x] Incidents
[x] DEPENDS_ON
[x] CAUSED_BY
[x] Peuplement du graphe
[x] Requêtes Cypher
[x] Dépendances amont
[x] Impacts aval
[x] Root cause
[x] Tests automatisés
[x] Suite complète de tests
[x] Git commit / push

STATUS: COMPLETED
```

---

# 25. Structure finale concernée

```text
ECDT/
│
├── data/
│   ├── ground_truth/
│   │   └── target_incidents.csv
│   │
│   └── processed/
│       └── topology/
│           ├── dependencies.csv
│           ├── services.csv
│           └── re2ob_checkoutservice_delay_topology.csv
│
├── src/
│   └── knowledge_graph/
│       ├── __init__.py
│       ├── neo4j_client.py
│       ├── graph_schema.py
│       ├── graph_builder.py
│       └── graph_queries.py
│
└── tests/
    └── test_knowledge_graph.py
```

---

# 26. Apport pour l'architecture ECDT

Cette étape fournit maintenant une représentation structurée de l'infrastructure pouvant être exploitée par les composants futurs.

La chaîne devient :

```text
RCAEval
   |
   v
Phase 1
Dataset / Topology
   |
   v
Phase 2
Ingestion / Normalization
/ Anomaly Detection
   |
   v
Phase 3
Knowledge Graph
   |
   +---- Services
   +---- Dependencies
   +---- Incidents
   +---- Root Causes
   |
   v
Future RCA
   |
   +---- Observer
   +---- Diagnostic / Impact
   +---- Memory
   +---- Recommendation
   +---- Orchestrator
```

Le Knowledge Graph constitue donc la couche structurelle permettant aux futurs composants de raisonner sur les dépendances et la propagation potentielle des incidents.

---

# 27. Limites actuelles

Cette étape ne constitue pas encore le moteur complet de RCA.

Le graphe actuel représente principalement :

```text
topologie
+
ground truth
```

Il ne déduit pas encore automatiquement :

- une root cause à partir des seules métriques ;
- une propagation causale réelle ;
- une anomalie temporelle dans le graphe ;
- des relations `Pod`, `Database` ou `Node` non présentes dans les données validées ;
- une causalité entre différentes anomalies.

Ces fonctionnalités appartiennent aux étapes suivantes de l'architecture ECDT.

---

# 28. Conclusion

La Phase 3 a permis de transformer la topologie et le ground truth validés lors des étapes précédentes en un Knowledge Graph Neo4j opérationnel.

Le système dispose désormais de :

```text
39 services
60 incidents
64 dépendances
60 relations de root cause
```

Le graphe est :

- peuplé ;
- interrogeable en Cypher ;
- idempotent au niveau du peuplement ;
- contrôlé par des tests automatisés ;
- intégré à la suite de tests du projet.

La Phase 3 est donc considérée comme **terminée et validée**.

La prochaine étape peut être abordée sans modifier les composants validés de cette phase.
