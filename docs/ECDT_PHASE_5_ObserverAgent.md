# **1\. Overview de l'Étape 5**

L'Observer est volontairement limité à la **détection déjà produite par la Phase 2, à la qualification et à la persistance de l'incident**.

Il ne réalise pas encore de Root Cause Analysis.

# **2\. Objectifs de l'Étape 5**

Les objectifs définis pour cette étape sont :

- \[x\] Consommer les anomalies produites par la Phase 2.
- \[x\] Définir un contrat d'entrée pour l'Observer.
- \[x\] Récupérer le contexte temporel depuis TimescaleDB.
- \[x\] Qualifier une anomalie.
- \[x\] Déterminer un type d'incident.
- \[x\] Déterminer une sévérité.
- \[x\] Calculer une confiance déterministe.
- \[x\] Générer un identifiant d'incident déterministe.
- \[x\] Construire un objet Incident structuré.
- \[x\] Persister l'incident dans Neo4j.
- \[x\] Relier l'incident à la ressource affectée.
- \[x\] Produire un IncidentContext destiné aux agents suivants.
- \[x\] Tester les composants individuellement.
- \[x\] Tester l'intégration réelle TimescaleDB + Neo4j.
- \[x\] Vérifier la structure finale du graphe.

## **Livrable**

Un Observer Agent fonctionnel capable de transformer une anomalie Phase 2 en Incident structuré, enrichi par son contexte temporel, persisté dans Neo4j et relié à la ressource concernée.

# **3\. Positionnement dans l'architecture ECDT**

Les phases précédentes fournissent les deux sources principales utilisées par l'Observer :
```
Phase 2
Ingestion
Normalization
Anomaly Detection
       |
       | AnomalyEvent
       v
   Observer Agent
       |
       +--------------------+
       |                    |
       v                    v
 TimescaleDB             Neo4j
 temporal context        topology
       |                    |
       +---------+----------+
                 |
                 v
          IncidentContext
                 |
                 v
       Diagnostic / Impact
```
La séparation des responsabilités est volontaire :
```
Phase 2
    -> détecte l’anomalie

Phase 5
    -> qualifie l’anomalie
    -> crée l’incident
    -> persiste l’incident

Future Diagnostic Agent
    -> recherche la cause racine

Future Impact Agent
    -> analyse la propagation

Future Recommendation Agent
    -> propose une action
```
L'Observer ne doit donc pas anticiper les responsabilités des agents suivants.

# **4\. Architecture de l'Étape 5**

La structure implémentée est :
```
src/
└── agents/
    └── observer/
        ├── __init__.py
        ├── models.py
        ├── incident_builder.py
        ├── timescale_consumer.py
        ├── incident_persistence.py
        └── observer_agent.py
```
Les tests associés sont :
```
tests/
├── test_incident_builder.py
├── test_incident_persistence.py
├── test_incident_persistence_integration.py
├── test_observer_agent.py
└── test_observer_agent_integration.py
```
# **5\. Découpage de l'Étape 5**

L'étape est organisée en sous-étapes :
```
5.1 — Modèles de données de l'Observer
5.2 — Qualification et construction d'Incident
5.3 — Consommation temporelle TimescaleDB
5.4 — Persistance Neo4j
5.5 — Orchestration ObserverAgent
5.6 — Tests et validation finale
```
# **6\. Étape 5.1 — Modèles de données**

## **6.1. Fichier**
```
src/agents/observer/models.py
```
Ce module définit les contrats de données de la couche Observer.

Le pipeline de données est :
```
AnomalyEvent
      |
      v
AnomalyInput
      |
      v
Incident
      |
      v
IncidentContext
```
## **6.2. IncidentStatus**

L'état actuel de l'incident est défini par :
```
classIncidentStatus(str,Enum):DETECTED="detected"
```
L'état DETECTED représente un incident nouvellement qualifié par l'Observer.

D'autres états pourront être ajoutés ultérieurement si le cycle de vie des incidents est étendu.

## **6.3. IncidentSeverity**

Les niveaux disponibles sont :
```
LOW
MEDIUM
HIGH
CRITICAL
```
Ils sont déterminés à partir du score d'anomalie.

## **6.4. IncidentSource**

La source actuelle est :
```
ECDT_OBSERVER
```
Elle permet d'identifier qu'un incident a été créé par l'Observer Agent.

# **7\. AnomalyInput**

AnomalyInput représente l'entrée normalisée de l'Observer.

Structure conceptuelle :
```
event_id
case_id
timestamp
resource_id
signal_type
metric_name
value
score
detection_method
incident_type
threshold
metadata
```
Le modèle fait notamment la distinction entre :
```
Phase 2 : service
```
et :
```
Observer : resource_id
```
Cette adaptation permet de conserver le vocabulaire propre à la couche cognitive.

# **8\. Incident**

L'objet Incident représente l'événement qualifié par l'Observer.

Il contient notamment :
```
incident_id
case_id
incident_type
status
severity
resource_id
detected_at
signal_type
metric_name
observed_value
anomaly_score
detection_method
confidence
source
metadata
```
Le modèle représente donc une anomalie devenue un objet métier persistant.

# **9\. IncidentContext**

IncidentContext est le contrat de sortie de l'Observer.

Il contient :
```
incident
resource_id
detection_timestamp
signal_type
metric_name
observed_value
anomaly_score
temporal_context
graph_context
metadata
qualification
persisted
incident_id
case_id
```
Il est explicitement conçu pour être transmis aux futurs agents :
```
IncidentContext
       |
       +--> Diagnostic Agent
       |
       +--> Impact Agent
```
Le modèle sépare volontairement :
```
qualification
temporal_context
graph_context
```
afin d'éviter de mélanger les responsabilités cognitives.

# **10\. Étape 5.2 — Qualification et construction d'Incident**

## **10.1. Fichier**
```
src/agents/observer/incident_builder.py
```
Le rôle du builder est de transformer :
```
AnomalyInput
```
en :
```
Incident
```
Le builder ne :

- consulte pas TimescaleDB ;
- consulte pas Neo4j ;
- réalise pas de RCA ;
- détermine pas une root cause.

Il réalise uniquement la qualification déterministe de l'anomalie.

# **11\. Génération de l'identifiant d'incident**

La fonction :
```
generate_incident_id(anomaly)
```
génère un identifiant déterministe.

L'identifiant est calculé à partir de :
```
case_id
event_id
timestamp
```
Un hash SHA-256 tronqué est utilisé.

Format :
```
inc_<short_hash>
```
Propriété importante :
```
même anomalie
      |
      v
même incident_id
```
Cette propriété est nécessaire pour permettre une persistance idempotente dans Neo4j.

# **12\. Détermination de la sévérité**

La fonction :
```
determine_severity(score)
```
utilise les seuils suivants :

| **Score**       | **Sévérité** |
| --------------- | ------------ |
| < 3             | LOW          |
| 3 <= score < 6  | MEDIUM       |
| 6 <= score < 10 | HIGH         |
| \>= 10          | CRITICAL     |

Un score négatif est rejeté.

Exemples :
```
2.99 -> LOW
3.0 -> MEDIUM
6.0 -> HIGH
10.0 -> CRITICAL
```
# **13\. Détermination de la confiance**

La fonction :
```
determine_confidence(score,detection_method
)
```
retourne toujours une valeur dans :
```
[0.0, 1.0]
```
## **Détection par seuil**

Pour :
```
DetectionMethod.THRESHOLD
```
la confiance est :
```
1.0
```
## **Détection par Z-score**

Pour :
```
DetectionMethod.Z_SCORE
```
la confiance est :
```
min(score / 10.0, 1.0)
```
Exemples :
```
score = 2  -> confidence = 0.2
score = 5  -> confidence = 0.5
score = 10 -> confidence = 1.0
score > 10 -> confidence = 1.0
```
Cette règle est volontairement simple et déterministe pour cette première version.

# **14\. Détermination du type d'incident**

Si la Phase 2 fournit déjà :
```
incident_type
```
celui-ci est conservé.

Sinon, l'Observer utilise le signal normalisé.

Mapping :
```
cpu
    -> CPU_SATURATION

latency_50
latency_90
latency-50
latency-90
latency
    -> DB_LATENCY

socket
error
network
    -> NETWORK_FAILURE
```
Cette logique permet de réutiliser les classifications déjà définies lors de la Phase 2.

# **15\. Construction de l'Incident**

La fonction :
```
build_incident(anomaly)
```
effectue les validations suivantes :
```
resource_id non vide
case_id non vide
metric_name non vide
value valide
score >= 0
```
Elle détermine ensuite :
```
incident_type
severity
confidence
incident_id
```
et construit :
```
Incident
```
avec :
```
status = DETECTED
source = ECDT_OBSERVER
```
# **16\. Étape 5.3 — Consommation TimescaleDB**

## **16.1. Fichier**
```
src/agents/observer/timescale_consumer.py
```
Le TimescaleConsumer est un composant en lecture seule.

Il réutilise les composants de l'Étape 4 :
```
TimescaleClient

timeseries_queries.py
```
Le principe est :
```
AnomalyInput
      |
      v
TimescaleConsumer
      |
      v
get_metrics_around_timestamp()
      |
      v
TimescaleDB
      |
      v
TemporalContext
```
# **17\. Responsabilités du TimescaleConsumer**

Le composant est responsable de :

- convertir le timestamp en datetime UTC ;
- rechercher les observations autour de l'anomalie ;
- filtrer les observations par cas ;
- filtrer les observations par métrique ;
- calculer des statistiques temporelles ;
- produire un TemporalContext.

Il ne :

- détecte pas les anomalies ;
- crée pas de nœuds Neo4j ;
- réalise pas de RCA ;
- exécute pas directement du SQL brut ;
- crée pas une nouvelle architecture de stockage.

# **18\. Fenêtre temporelle**

La fenêtre par défaut utilisée par l'Observer est :
```
5 minutes avant
+
5 minutes après
```
soit :
```
window_before_seconds = 300

window_after_seconds = 300
```
La valeur est configurable avec :
```
window_minutes
```
et une valeur négative est rejetée.

# **19\. Normalisation des timestamps**

La Phase 2 peut utiliser des timestamps Unix en millisecondes.

L'Observer normalise donc les timestamps à sa frontière :
```
Phase 2
milliseconds
     |
     v
ObserverAgent
     |
     v
seconds
     |
     v
TimescaleConsumer
     |
     v
UTC datetime
```
L'Observer accepte également les timestamps déjà exprimés en secondes.

La détection du format est basée sur l'ordre de grandeur du timestamp.

# **20\. Filtrage du contexte temporel**

Les observations récupérées sont filtrées successivement par :
```
case_id
    |
    v
metric_name
```
Le pipeline est :
```
Observations TimescaleDB
        |
        v
Même case_id
        |
        v
Même metric_name
        |
        v
TemporalContext
```
Cela empêche de mélanger le contexte de plusieurs cas RCAEval ou plusieurs métriques.

# **21\. Statistiques temporelles**

Le TimescaleConsumer calcule :
```
observation_count

minimum

maximum

mean

first_value

last_value

delta
```
et ajoute également :
```
anomaly_value

anomaly_score
```
Ces statistiques sont destinées à être exploitées par les futurs agents cognitifs.

# **22\. TemporalContext**

Le contexte temporel contient :
```
resource_id

metric_name

signal_type

anomaly_timestamp

window_before_seconds

window_after_seconds

observations

statistics
```
Exemple conceptuel :
```
TemporalContext
├── resource_id
├── metric_name
├── signal_type
├── anomaly_timestamp
├── window_before_seconds
├── window_after_seconds
├── observations
└── statistics
    ├── observation_count
    ├── minimum
    ├── maximum
    ├── mean
    ├── first_value
    ├── last_value
    ├── delta
    ├── anomaly_value
    └── anomaly_score
```
# **23\. Étape 5.4 — Persistance Neo4j**

## **23.1. Fichier**
```
src/agents/observer/incident_persistence.py
```
Ce module est responsable de l'écriture des incidents dans le Knowledge Graph.

La séparation avec :
```
src/knowledge_graph/graph_queries.py
```
est volontaire.

graph_queries.py reste principalement dédié aux requêtes de lecture, tandis que IncidentPersistence porte les opérations d'écriture nécessaires à l'Observer.

# **24\. Création du nœud Incident**

L'opération :
```
create_incident(incident)
```
utilise :
```
MERGE (i:Incident {id: $incident_id})
```
Puis les propriétés sont mises à jour.

Les propriétés persistées comprennent notamment :
```
case_id

incident_type

status

severity

resource_id

detected_at

signal_type

metric_name

observed_value

anomaly_score

detection_method

confidence

source

metadata
```
L'utilisation de MERGE permet une opération idempotente sur l'identifiant d'incident.

# **25\. Relation AFFECTS**

L'Observer crée la relation :
```
(:Incident)-[:AFFECTS]->(:Service)
```
Cette relation signifie :

l'Observer a détecté une anomalie affectant cette ressource.

Exemple :
```
Incident
   |
   | AFFECTS
   v
checkoutservice
```
# **26\. Distinction entre AFFECTS et CAUSED_BY**

Cette distinction est essentielle.

## **AFFECTS**
```
(:Incident)-[:AFFECTS]->(:Service)
```
Signifie :
```
la ressource est affectée par l'incident détecté
```
Cette relation peut donc être créée par l'Observer.

## **CAUSED_BY**
```
(:Incident)-[:CAUSED_BY]->(:Service)
```
Signifie :
```
le service est établi comme root cause
```
Cette relation ne doit pas être inventée par l'Observer.

La root cause sera déterminée par un futur agent de Diagnostic et/ou par une procédure d'évaluation utilisant la vérité terrain.

# **27\. Protection contre les ressources inexistantes**

Lors de la persistance complète :
```
persist_incident(incident)
```
le service est recherché avant la création de l'incident.

Principe :
```
MATCH (s:Service {id: $resource_id})
MERGE (i:Incident {id: $incident_id})
...
MERGE (i)-[:AFFECTS]->(s)
```
Si la ressource n'existe pas :
```
pas de persistance valide

pas de nœud Incident orphelin
```
Cette règle empêche l'Observer d'inventer une topologie.

# **28\. Persistance complète**

La méthode :
```
persist_incident(incident)
```
effectue conceptuellement :
```
Incident
   |
   +--> vérifier Service
   |
   +--> MERGE Incident
   |
   +--> SET propriétés
   |
   +--> MERGE AFFECTS
   |
   v
Neo4j
```
Elle retourne une structure contenant :
```
incident

relationship
```
# **29\. Vérification après persistance**

La classe fournit également :
```
get_incident(incident_id)
```
pour rechercher un incident persistant.

Elle fournit également :
```
incident_affects_resource(incident_id,resource_id

)
```

pour vérifier explicitement la relation :
```
Incident -[:AFFECTS]-> Service
```
# **30\. Sérialisation des métadonnées**

Neo4j n'accepte pas directement un dictionnaire Python comme propriété.

Les métadonnées sont donc sérialisées en JSON :

```
json.dumps(dict(incident.metadata),default=str,)
```
La propriété Neo4j :
```
metadata
```
est ainsi stockée sous une forme sérialisée.

# **31\. Étape 5.5 — ObserverAgent**

## **31.1. Fichier**
```
src/agents/observer/observer_agent.py
```
L'ObserverAgent est l'orchestrateur de la couche Observer.

Son pipeline est :
```
AnomalyEvent
      |
      v
AnomalyInput
      |
      v
TimescaleConsumer
      |
      v
TemporalContext
      |
      v
Qualification
      |
      v
IncidentBuilder
      |
      v
Incident
      |
      v
IncidentPersistence
      |
      v
Neo4j
      |
      v
IncidentContext
```
# **32\. Initialisation de l'ObserverAgent**

Le constructeur reçoit :
```
ObserverAgent(timescale_consumer,incident_persistence,window_minutes=5,minimum_confidence=0.0,

)
```
Les validations suivantes sont effectuées :
```
timescale_consumer != None

incident_persistence != None

window_minutes >= 0

0.0 <= minimum_confidence <= 1.0
```
Une valeur de confiance hors intervalle est rejetée.

# **33\. Conversion AnomalyEvent → AnomalyInput**

La méthode :
```
_to_anomaly_input(anomaly)
```
adapte le modèle de Phase 2 au contrat de l'Observer.

Correspondances principales :
```
Phase 2 service
       |
       v
Observer resource_id

Phase 2 timestamp
       |
       v
Observer timestamp normalisé en secondes

Phase 2 signal_type
       |
       v
Observer signal_type

Phase 2 value
       |
       v
Observer value

Phase 2 score
       |
       v
Observer score
```
# **34\. Validation de l'entrée**

L'Observer refuse :
```
anomaly = None
```
Il refuse également une anomalie dont :
```
service est vide
```
et une anomalie marquée :
```
is_anomaly = False
```
L'Observer ne traite donc que des événements explicitement identifiés comme anomalies par la couche précédente.

# **35\. Qualification**

La méthode :
```
_qualify(anomaly,temporal_context)
```

retourne notamment :
```
is_qualified

confidence

observation_count

detection_method

score

resource_id

signal_type

metric_name
```
La confiance est calculée à partir de la même règle déterministe que celle du IncidentBuilder.

Cela évite d'avoir deux règles différentes entre :
```
qualification
```
et :
```
construction de l'incident
```
# **36\. Seuil de confiance**

Le paramètre :
```
minimum_confidence
```
permet de contrôler si une anomalie peut devenir un incident.

Principe :
```
confidence < minimum_confidence
        |
        v
rejet
```
Sinon :
```
confidence >= minimum_confidence
        |
        v
Incident
```
Le comportement par défaut est :
```
minimum_confidence = 0.0
```
Ce choix permet de ne pas supprimer silencieusement les anomalies déjà détectées par la Phase 2.

# **37\. Construction de l'Incident**

Une fois la qualification validée :
```
incident=build_incident(anomaly_input

)
```
L'Incident reçoit notamment :
```
incident_id

case_id

incident_type

status

severity

resource_id

detected_at

signal_type

metric_name

observed_value

anomaly_score

detection_method

confidence

source

metadata
```
# **38\. Persistance**

L'Observer appelle :
```
self.incident_persistence.persist_incident(incident)
```
L'incident est alors écrit dans Neo4j et relié à sa ressource.

Le pipeline réel est donc :
```
Phase 2 anomaly
       |
       v
Observer
       |
       v
TimescaleDB context
       |
       v
Incident
       |
       v
Neo4j
       |
       +--> Incident node
       |
       +--> AFFECTS
```
# **39\. Production de l'IncidentContext**

Après persistance, l'Observer retourne :
```
IncidentContext
```
avec notamment :
```
incident

resource_id

detection_timestamp

signal_type

metric_name

observed_value

anomaly_score

temporal_context

qualification

persisted = True

incident_id

case_id
```
Ce contexte est le contrat de sortie destiné aux agents cognitifs suivants.

# **40\. Limite fonctionnelle volontaire**

L'Observer ne réalise pas :
```
Root Cause Analysis
```
Il ne décide donc pas :
```
Incident X CAUSED_BY Service Y
```
à partir de son seul contexte temporel.

Il établit uniquement :
```
Incident X AFFECTS Service Y
```
La cause racine doit être déterminée ultérieurement par le Diagnostic Agent à partir de :
```
Knowledge Graph
+
Temporal history
+
Observability signals
+
Ground truth for evaluation
```
# **41\. Étape 5.6 — Tests**

La validation de l'Étape 5 est organisée sur plusieurs niveaux.
```
Unit tests
    |
    +--> Incident Builder
    +--> Incident Persistence
    +--> Observer Agent
    +--> Timescale Consumer
    |
    v
Integration tests
    |
    +--> TimescaleDB
    +--> Neo4j
    +--> ObserverAgent
    |
    v
Acceptance test
```
# **42\. Tests du IncidentBuilder**

Fichier :
```
tests/test_incident_builder.py
```
Les tests couvrent notamment :

- les niveaux de sévérité ;
- la détermination du type CPU ;
- la détermination de l'identifiant déterministe ;
- le changement d'identifiant lorsque l'événement change ;
- la construction d'un incident ;
- la construction d'un IncidentContext ;
- le rejet d'un score négatif ;
- la confiance pour les détections par seuil ;
- la confiance bornée pour les Z-scores.

Le fichier contient 12 tests.

# **43\. Tests de persistance**

Fichier :
```
tests/test_incident_persistence.py
```
Les tests couvrent notamment :
```
client Neo4j obligatoire

création d'incident

utilisation de incident_id

liaison Incident -> Service

rejet d'une ressource inexistante

persistance complète

récupération d'un incident

absence d'un incident inconnu

vérification AFFECTS = True

vérification AFFECTS = False

sérialisation des paramètres
```
Ils vérifient que la couche de persistance respecte le contrat attendu sans nécessiter Neo4j réel pour les tests unitaires.

# **44\. Tests d'intégration de persistance**

Fichier :

tests/test_incident_persistence_integration.py

Ces tests valident la persistance avec une instance Neo4j réelle.

Ils vérifient notamment :
```
Service réel
      |
      v
Incident réel
      |
      v
AFFECTS
      |
      v
Service
```
Ils permettent de distinguer les problèmes de logique Python des problèmes réels de connexion ou de requête Cypher.

# **45\. Tests unitaires de l'ObserverAgent**

Fichier :
```
tests/test_observer_agent.py
```
Les dépendances externes sont mockées.

Les tests couvrent :
```
validation du constructeur

validation de window_minutes

validation de minimum_confidence

conversion AnomalyEvent -> AnomalyInput

normalisation millisecondes -> secondes

qualification

confiance bornée

orchestration complète

lecture du contexte temporel

persistance de l'incident

préservation du TemporalContext

contenu de la qualification

rejet d'une confiance insuffisante

rejet d'une anomalie None

cohérence des identifiants
```
Le test suite permet donc de vérifier l'orchestration sans dépendre des services Docker.

# **46\. Tests d'intégration de l'ObserverAgent**

Fichier :
```
tests/test_observer_agent_integration.py
```
Ces tests utilisent :
```
TimescaleDB réel
+
Neo4j réel
+
ObserverAgent réel
```
Le cas RCAEval utilisé est :
```
re2ob_checkoutservice_cpu_1
```
avec :
```
resource_id = checkoutservice

signal_type = cpu

metric_name = checkoutservice_cpu
```
# **47\. Pipeline d'intégration réel**

Le test d'acceptation exécute :
```
TimescaleDB
    |
    v
observation réelle
    |
    v
AnomalyEvent
    |
    v
ObserverAgent
    |
    +--> TemporalContext
    |
    +--> Incident
    |
    v
Neo4j
    |
    v
(:Incident)-[:AFFECTS]->(:Service)
```
Ce test vérifie la chaîne complète et non uniquement les composants isolés.

# **48\. Critère d'acceptation final**

Le critère principal de l'Étape 5.5 est :
```
(:Incident)-[:AFFECTS]->(:Service)
```
Pour le cas :
```
re2ob_checkoutservice_cpu_1
```
le graphe attendu est :
```
Incident
    |
    | AFFECTS
    v
checkoutservice
```
avec l'incident associé au même :
```
case_id
```
et :
```
resource_id
```
# **49\. Validation finale rapportée**

La validation finale de l'ObserverAgent a été réalisée avec les tests d'intégration.

Le test d'acceptation comprend six validations :
```
1\. observation réelle disponible dans TimescaleDB

2\. service réel disponible dans Neo4j

3\. ObserverAgent traite l'anomalie

4\. Incident présent dans Neo4j

5\. relation AFFECTS présente

6\. test d'acceptation global du pipeline
```
Résultat rapporté :
```
6 passed
```
Cela confirme le fonctionnement de la chaîne intégrée utilisée pour l'Observer.

# **50\. Validation de l'intégration TimescaleDB**

Le contexte temporel provient réellement de :
```
TimescaleDB
```
et non d'un contexte artificiel dans le test d'intégration.

La couche utilise le client existant de l'Étape 4 :
```
src/digital_twin/timescale_client.py
```
et la requête :
```
get_metrics_around_timestamp()
```
Cette réutilisation évite de créer une seconde implémentation de la couche temporelle.

# **51\. Validation de l'intégration Neo4j**

Le nœud :
```
(:Incident)
```
est réellement créé dans Neo4j.

La relation :
```
(:Incident)-[:AFFECTS]->(:Service)
```
est également réellement vérifiée.

Le test final vérifie :
```
incident_id

case_id

resource_id

relationship = AFFECTS

service_id
```
# **52\. Cohérence de l'identité d'un incident**

L'identité est propagée de bout en bout :
```
AnomalyEvent.event_id
        |
        v
AnomalyInput.event_id
        |
        v
generate_incident_id()
        |
        v
Incident.incident_id
        |
        v
Neo4j Incident.id
        |
        v
IncidentContext.incident_id
```
De même :
```
case_id
```
et :
```
resource_id
```
sont conservés tout au long du pipeline.

# **53\. Gestion de l'idempotence**

Deux mécanismes contribuent à l'idempotence :

## **Identifiant déterministe**

La même anomalie produit le même :
```
incident_id
```
## **MERGE Neo4j**

La persistance utilise :
```
MERGE (i:Incident {id: $incident_id})
```
et :
```
MERGE (i)-[:AFFECTS]->(s)
```
Ainsi, le même événement peut être retraité sans créer plusieurs incidents identiques ni plusieurs relations AFFECTS identiques.

# **54\. Flux complet avec un cas réel**

Cas :
```
re2ob_checkoutservice_cpu_1
```
Ressource :
```
checkoutservice
```
Signal :
```
cpu
```
Métrique :
```
checkoutservice_cpu
```
Flux :
```
Phase 2
AnomalyEvent
      |
      v
ObserverAgent
      |
      v
AnomalyInput
      |
      v
TimescaleDB
      |
      v
TemporalContext
      |
      v
Qualification
      |
      v
Incident
      |
      v
Neo4j
      |
      v
IncidentContext
```
# **55\. Exemple de structure logique de l'incident**

Pour un incident CPU, la structure conceptuelle est :
```
Incident
├── incident_id
├── case_id
│   └── re2ob_checkoutservice_cpu_1
├── incident_type
│   └── CPU_SATURATION
├── status
│   └── detected
├── severity
├── resource_id
│   └── checkoutservice
├── detected_at
├── signal_type
│   └── cpu
├── metric_name
│   └── checkoutservice_cpu
├── observed_value
├── anomaly_score
├── detection_method
├── confidence
├── source
│   └── ECDT_OBSERVER
└── metadata
```
# **56\. Ce que l'Étape 5 apporte au Digital Twin**

Avant l'Étape 5 :
```
TimescaleDB
    |
    v
anomalies
```
Après l'Étape 5 :
```
TimescaleDB
    |
    v
AnomalyEvent
    |
    v
Observer
    |
    v
Incident
    |
    v
Neo4j
```
Le système possède donc désormais un pont entre :
```
données temporelles
```
et :
```
représentation sémantique des incidents
```
# **57\. Ce qui n'est pas encore réalisé**

L'Étape 5 ne constitue pas encore le système RCA complet.

Les fonctions suivantes restent à réaliser dans les étapes suivantes :
```
Root Cause Analysis
        |
        v
Diagnostic Agent

Propagation d’impact
        |
        v
Impact Agent

Explication / recommandation
        |
        v
Recommendation Agent

Recherche d’incidents similaires
        |
        v
Memory Agent

Orchestration multi-agents
        |
        v
Cognitive Orchestrator
```
L'Observer constitue le premier maillon de cette architecture.

# **58\. Limitation importante de la qualification**

La confiance actuelle est déterministe et dépend principalement de :
```
detection_method
+
anomaly_score
```
Le TemporalContext est récupéré et transmis, mais n'est volontairement pas utilisé pour calculer une nouvelle confiance complexe.

Cette décision permet de conserver une séparation claire :
```
Observer
    -> qualification

Diagnostic
    -> raisonnement causal
```
Une stratégie de confiance plus avancée pourra être introduite plus tard si elle est justifiée par les résultats expérimentaux.

# **59\. Limitation concernant les signaux**

Le mapping des types d'incidents est hérité de la Phase 2 :
```
cpu
    -> CPU_SATURATION

latency
    -> DB_LATENCY

socket / error / network
    -> NETWORK_FAILURE
```
La validation statistique de la Phase 2 a montré que la détection peut être validée indépendamment de l'identification sémantique du signal causal.

Il faut donc continuer à distinguer :
```
détection d'une anomalie
```
de :
```
qualification sémantique
```
et de :
```
RCA
```
# **60\. Relation avec le Ground Truth**

Le ground truth RCAEval contient :
```
root_cause_service
```
mais l'Observer ne doit pas utiliser cette information pour fabriquer artificiellement une relation :
```
CAUSED_BY
```
Le ground truth est principalement utilisé pour :
```
évaluation
```
et :
```
validation future du Diagnostic Agent
```
L'Observer conserve uniquement les informations nécessaires à l'identification de l'incident et à la ressource affectée.

# **61\. Architecture de sortie vers les futurs agents**

La sortie de l'Observer est :
```
IncidentContext
```
Elle peut être représentée ainsi :
```
                         IncidentContext
                               |
             +-----------------+-----------------+
             |                 |                 |
             v                 v                 v
          Incident      TemporalContext     GraphContext
             |                 |                 |
             |                 |                 |
             v                 v                 v
       Incident data      TimescaleDB        Neo4j
             |                 |                 |
             +-----------------+-----------------+
                               |
                               v
                      Diagnostic / Impact
```
Le champ graph_context est préparé par le contrat de sortie mais n'est pas encore alimenté par l'Observer actuel.

# **62\. État des composants**

```
src/agents/observer/
├── models.py
│   STATUS: COMPLETED
│
├── incident_builder.py
│   STATUS: COMPLETED
│
├── timescale_consumer.py
│   STATUS: COMPLETED
│
├── incident_persistence.py
│   STATUS: COMPLETED
│
└── observer_agent.py
    STATUS: COMPLETED
```

# **63\. Checklist finale de l'Étape 5**

## **5.1 — Modèles**

- \[x\] AnomalyInput
- \[x\] Incident
- \[x\] IncidentContext
- \[x\] TemporalContext
- \[x\] IncidentStatus
- \[x\] IncidentSeverity
- \[x\] IncidentSource

## **5.2 — Qualification**

- \[x\] génération d'ID déterministe
- \[x\] détermination de sévérité
- \[x\] détermination de confiance
- \[x\] détermination du type d'incident
- \[x\] construction d'Incident
- \[x\] construction d'IncidentContext

## **5.3 — TimescaleDB**

- \[x\] réutilisation du client Phase 4
- \[x\] récupération du contexte temporel
- \[x\] conversion UTC
- \[x\] fenêtre temporelle configurable
- \[x\] filtrage par case_id
- \[x\] filtrage par metric_name
- \[x\] statistiques temporelles

## **5.4 — Neo4j**

- \[x\] création d'Incident
- \[x\] persistance idempotente
- \[x\] relation AFFECTS
- \[x\] vérification de la ressource existante
- \[x\] récupération d'incident
- \[x\] vérification de AFFECTS
- \[x\] sérialisation des métadonnées
- \[x\] aucune création de CAUSED_BY par l'Observer

## **5.5 — ObserverAgent**

- \[x\] conversion Phase 2 → Observer
- \[x\] validation des entrées
- \[x\] récupération du contexte temporel
- \[x\] qualification
- \[x\] création de l'Incident
- \[x\] persistance Neo4j
- \[x\] création de l'IncidentContext

## **5.6 — Tests**

- \[x\] tests Incident Builder
- \[x\] tests Incident Persistence
- \[x\] tests Observer Agent
- \[x\] tests d'intégration TimescaleDB
- \[x\] tests d'intégration Neo4j
- \[x\] test d'acceptation du pipeline réel
- \[x\] validation finale rapportée : 6 tests d'intégration réussis

# **64\. Commandes de validation**

Depuis la racine du projet :

## **Tests unitaires**
```
python-mpytesttests/test_incident_builder.py-v

python-mpytesttests/test_incident_persistence.py-v

python-mpytesttests/test_observer_agent.py-v
```
## **Tests d'intégration**

Avant les tests réels :
```
dockercomposeup-dneo4jtimescaledb
```
Puis :
```
python-mpytesttests/test_incident_persistence_integration.py-v
```
et :
```
python-mpytesttests/test_observer_agent_integration.py-v
```
## **Suite complète**

python-mpytesttests-v

# **65\. Dépendances externes nécessaires**

Pour les tests d'intégration, les services suivants doivent être disponibles :

```bash
Neo4j
    localhost:7687

TimescaleDB
    localhost:5432
```

Les variables d'environnement utilisées par les tests sont notamment :

```
NEO4J_URI
TIMESCALE_URI
```

Si les services ne sont pas disponibles, les tests d'intégration ne représentent pas une validation de l'environnement réel.

# **66\. Résultat architectural final**

L'Étape 5 permet maintenant de passer de :
```bash
ANOMALY
```
à :
```bash
STRUCTURED INCIDENT
```
avec :
```bash
Temporal Context
+
Qualification
+
Persistence
+
Affected Resource
```

Le résultat est :
```bash
                 Phase 2
              AnomalyEvent
                   |
                   v
             ObserverAgent
                   |
        +----------+----------+
        |                     |
        v                     v
   TimescaleDB            Qualification
        |                     |
        v                     v
TemporalContext            Incident
        |                     |
        +----------+----------+
                   |
                   v
                 Neo4j
                   |
                   v
       Incident -[:AFFECTS]->
              Service
                   |
                   v
          IncidentContext
                   |
                   v
        Diagnostic / Impact
```
# **67\. Conclusion**

L'Étape 5 constitue la première implémentation fonctionnelle de la couche cognitive ECDT.

Les composants développés permettent désormais de :
```bash
recevoir une anomalie Phase 2

↓

la normaliser pour la couche cognitive

↓

récupérer son contexte temporel

↓

la qualifier

↓

construire un Incident

↓

le persister dans Neo4j

↓

le relier à la ressource affectée

↓

produire un IncidentContext
```

La responsabilité de l'Observer reste volontairement limitée :

```bash
Observer \= détection reçue \+ qualification \+ contextualisation temporelle + persistance
```
et non :
```bash
Observer != Root Cause Analysis
```
Cette séparation fournit le contrat nécessaire pour poursuivre l'architecture cognitive avec les futurs agents de **Diagnostic**, **Impact**, **Recommandation** et **Mémoire**.

# **68\. Statut final**

```bash
\==================================================

ECDT — ÉTAPE 5

COGNITIVE LAYER — OBSERVER

\==================================================

5.1 Observer Models \[COMPLETED\]

5.2 Incident Qualification / Builder \[COMPLETED\]

5.3 TimescaleDB Consumer \[COMPLETED\]

5.4 Neo4j Incident Persistence \[COMPLETED\]

5.5 ObserverAgent \[COMPLETED\]

5.6 Tests & Integration Validation \[COMPLETED\]

\--------------------------------------------------

*Observer pipeline:*

AnomalyEvent\-> AnomalyInput\-> TemporalContext\-> Qualification\-> Incident\-> Neo4j\-> IncidentContext

*Neo4j relationship:*

(:Incident)-\[:AFFECTS\]->(:Service)

*Root cause:*

NOT determined by Observer

*Final reported integration validation:*

6 passed

*STATUS:*

COMPLETED

\==================================================
```