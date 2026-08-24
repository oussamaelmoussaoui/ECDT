# ECDT — Phase 2 : Ingestion, Normalisation et Détection d'Anomalies

## 1. Vue d'ensemble

La Phase 2 transforme les données préparées de RCAEval en une représentation exploitable par ECDT.

La chaîne réalisée est :

```text
RCAEval / données préparées
        |
        v
DatasetLoader
        |
        v
Metrics / Logs / Traces
        |
        v
SchemaNormalizer
        |
        v
Normalized Events
        |
        v
Time Series
        |
        v
AnomalyDetector
        |
        v
Anomaly Events
        |
        v
Validation against Ground Truth
```

La Phase 1 avait produit un sous-ensemble de 60 incidents, le ground truth et les données d'observabilité. La documentation de Phase 1 confirme notamment les 60 cas : 15 CPU, 15 DELAY, 15 LOSS et 15 SOCKET. fileciteturn17file0L1-L25

---

## 2. Objectifs

Les objectifs de cette phase sont :

- fournir un loader centralisé ;
- charger les métriques, logs et traces par cas ;
- harmoniser les timestamps ;
- convertir les métriques du format large au format long ;
- normaliser les trois sources dans un schéma commun ;
- construire des séries temporelles ;
- calculer une baseline sur la période normale ;
- détecter statistiquement les anomalies ;
- préparer la validation contre le ground truth.

La Phase 2 n'est pas encore le moteur RCA final.

---

## 3. Données d'entrée

Les principaux fichiers sont :

```text
data/
├── processed/
│   ├── metrics/
│   │   └── target_metrics.csv
│   ├── logs/
│   │   └── target_logs.csv
│   └── traces/
│       └── target_traces.csv
│
└── ground_truth/
    └── target_incidents.csv
```

Le ground truth validé contient :

```text
60 lignes
13 colonnes
```

Colonnes :

```text
case
dataset
suite
system_name
fault
fault_description
root_cause_service
inject_time
time_start
time_end
duration_minutes
normal_timesteps
faulty_timesteps
```

Exemple :

```text
case                : re2ob_checkoutservice_cpu_1
dataset             : RE2-OB
fault               : cpu
root_cause_service  : checkoutservice
inject_time         : 1705354566
time_start          : 1705353846
time_end            : 1705355286
duration_minutes    : 24
normal_timesteps    : 720
faulty_timesteps    : 721
```

---

## 4. `dataset_loader.py`

Fichier :

```text
src/ingestion/dataset_loader.py
```

Le loader centralise l'accès aux données préparées.

Il prend notamment en charge :

- validation des fichiers ;
- chargement des métriques ;
- chargement des logs ;
- chargement des traces ;
- filtrage par `case_id` ;
- filtrage temporel ;
- format long des métriques ;
- récupération des informations du ground truth.

Création :

```python
from pathlib import Path
from src.ingestion.dataset_loader import create_default_loader

loader = create_default_loader(Path("."))
```

Validation effectuée :

```text
Cases: 60
```

Premiers cas :

```text
re2ob_checkoutservice_cpu_1
re2ob_checkoutservice_delay_1
re2ob_checkoutservice_loss_1
re2ob_checkoutservice_socket_1
re2ob_currencyservice_cpu_1
```

---

## 5. Validation d'un cas

Pour :

```text
re2ob_checkoutservice_cpu_1
```

le loader a fourni :

```text
Metrics : 785345 lignes
Logs    : 171322 lignes
Traces  : 391997 lignes
```

Les timestamps ont été contrôlés.

Ground truth :

```text
START  = 1705353846000
INJECT = 1705354566000
END    = 1705355286000
```

Données chargées :

```text
Metrics :
1705353846000 -> 1705355286000

Logs :
1705353846000 -> 1705355286000

Traces :
1705353846065 -> 1705355285970
```

La fenêtre temporelle est donc cohérente.

---

## 6. Format long des métriques

Les métriques originales sont en format large avec de nombreuses colonnes.

Le loader permet leur transformation en format long :

```text
case
dataset
fault
root_cause_service
time
timestamp
metric_name
value
```

Le type de `value` a été corrigé en :

```text
Float64
```

Cette modification est nécessaire pour les calculs statistiques.

---

## 7. Exemple : métrique CPU

Pour :

```text
checkoutservice_cpu
```

on obtient :

```text
SHAPE : (1441, 8)
MIN   : 1705353846000
MAX   : 1705355286000
```

Statistiques :

```text
MEAN      : 9.36574402159815
MIN VALUE : 0.18590000000000845
MAX VALUE : 20.031263076095414
```

Avant injection :

```text
rows : 720
mean : 0.42885440894546123
max  : 0.6869909369114802
```

Après injection :

```text
rows : 721
mean : 18.290238503026632
max  : 20.031263076095414
```

Cette séparation montre clairement le changement du comportement du signal.

---

## 8. `schema_normalizer.py`

Fichier :

```text
src/ingestion/schema_normalizer.py
```

Le normalizer transforme :

```text
metrics
logs
traces
```

en une représentation commune.

Le schéma validé contient 19 champs :

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

---

## 9. Extraction service / signal

Le parsing des métriques a été validé avec :

```text
adservice_cpu
    -> adservice / cpu

checkoutservice_mem
    -> checkoutservice / mem

carts-db_diskio
    -> carts-db / diskio

frontend_latency-50
    -> frontend / latency-50

ts-route-service_latency-90
    -> ts-route-service / latency-90

ts-auth-service_error
    -> ts-auth-service / error
```

Une incompatibilité initiale avec Polars 1.43.2 concernant `str.rsplit` a été corrigée avec des expressions basées sur `str.replace` et `str.extract`.

La normalisation des métriques a ensuite été validée.

---

## 10. Normalisation complète

Pour le cas CPU :

```text
Total events : 1348664
```

Répartition :

```text
metric : 785345
log    : 171322
trace  : 391997
```

Vérification :

```text
785345 + 171322 + 391997 = 1348664
```

Les trois sources sont donc bien représentées dans le schéma canonique.

---

## 11. `anomaly_detector.py`

Fichier :

```text
src/ingestion/anomaly_detector.py
```

Le détecteur implémente notamment :

```text
build_series()
compute_baseline()
detect_series()
zscore()
detect_in_events()
```

Pipeline :

```text
NormalizedEvent
      |
      v
TimeSeries
      |
      v
Baseline
      |
      v
Z-score / threshold
      |
      v
AnomalyEvent
```

Le code documente explicitement la règle suivante :

> la baseline doit être construite exclusivement à partir des observations précédant l'injection.

---

## 12. Mapping des incidents

Le mapping retenu est :

```text
CPU
    -> CPU_SATURATION

DELAY
    -> DB_LATENCY

LOSS
    -> NETWORK_FAILURE

SOCKET
    -> NETWORK_FAILURE
```

Signaux principaux :

```text
CPU_SATURATION
    -> cpu

DB_LATENCY
    -> latency-50
    -> latency-90

NETWORK_FAILURE
    -> socket
    -> error
```

---

## 13. Séries temporelles

La méthode :

```python
build_series(events)
```

regroupe les événements métriques par :

```text
(service_name, signal_type)
```

Sur le cas CPU, le résultat a été :

```text
SERIES : 72
```

Exemples :

```text
('checkoutservice', 'cpu')
('currencyservice', 'cpu')
('frontend', 'cpu')
('emailservice', 'cpu')
('checkoutservice', 'latency-50')
('checkoutservice', 'latency-90')
```

---

## 14. Baseline

La baseline est calculée uniquement avant :

```text
inject_time
```

Pour :

```text
re2ob_checkoutservice_cpu_1
```

résultat :

```text
Baseline samples : 720
Mean             : 0.4288544084
Std              : 0.0788268345
Max              : 0.6869909369
```

Cette séparation évite d'utiliser le comportement fautif pour définir la normalité.

---

## 15. Z-score

Le calcul est :

```text
z = (value - mean) / std
```

Tests :

```text
z(10, 10, 1) = 0
z(11, 10, 1) = 1
z(13, 10, 1) = 3
z(7, 10, 1)  = -3
```

Seuil utilisé :

```text
|z| >= 3
```

---

## 16. Validation CPU

Cas :

```text
re2ob_checkoutservice_cpu_1
```

Résultats :

```text
Series points       : 1441
Baseline samples    : 720
Baseline mean       : 0.428854
Baseline std        : 0.078827
Baseline max        : 0.686991

Total anomalies     : 707
Before injection    : 0
After injection     : 707

First detection     : 1705354580000
Detection delay     : 14.00 s
Last detection      : 1705355286000

First anomaly value : 5.632196
First anomaly score : 66.009773
```

Le détecteur ne produit donc aucune anomalie avant l'injection sur ce cas.

---

## 17. Observations manquées

Un test supplémentaire a trouvé :

```text
MISSED : 14
```

Les valeurs manquées au tout début de la période fautive étaient comprises entre :

```text
0.390501
et
0.473909
```

Elles sont proches de la baseline.

Cela explique pourquoi un seuil `|z| >= 3` ne les marque pas immédiatement.

La première vraie détection arrive donc 14 secondes après l'injection.

---

## 18. Validation des quatre incidents

Le test reproductible est :

```powershell
python -m tests.test_incidents
```

L'exécution directe :

```powershell
python tests/test_incidents.py
```

avait provoqué :

```text
ModuleNotFoundError: No module named 'src'
```

Le lancement en module a résolu le problème.

Résultats obtenus :

| Incident | Anomalies | Avant injection | Après injection | Délai |
|---|---:|---:|---:|---:|
| CPU | 707 | 0 | 707 | 14 s |
| DELAY | 15 | 0 | 15 | 188 s |
| LOSS | 51 | 0 | 51 | 58 s |
| SOCKET | 236 | 0 | 236 | 7 s |

### CPU

```text
Baseline mean : 0.428854
Baseline std  : 0.078827
Anomalies     : 707
Before        : 0
After         : 707
Delay         : 14 s
```

### DELAY

```text
Baseline mean : 0.422945
Baseline std  : 0.075131
Anomalies     : 15
Before        : 0
After         : 15
Delay         : 188 s
```

### LOSS

```text
Baseline mean : 0.428367
Baseline std  : 0.084800
Anomalies     : 51
Before        : 0
After         : 51
Delay         : 58 s
```

### SOCKET

```text
Baseline mean : 0.393426
Baseline std  : 0.075372
Anomalies     : 236
Before        : 0
After         : 236
Delay         : 7 s
```

---

## 19. Interprétation

La propriété la plus importante vérifiée sur les quatre cas est :

```text
Before injection = 0
```

et :

```text
After injection > 0
```

pour chaque incident.

Cela valide le fonctionnement de base de la détection temporelle.

La validation démontre la chaîne :

```text
Dataset
   |
   v
Loader
   |
   v
Normalization
   |
   v
Metric Series
   |
   v
Baseline
   |
   v
Z-score
   |
   v
Anomaly Detection
```

---

## 20. Limitation importante

Les tests fournis pour DELAY, LOSS et SOCKET ont utilisé :

```text
('checkoutservice', 'cpu')
```

comme série de validation.

Cela valide le fonctionnement général du détecteur, mais ne constitue pas encore une validation sémantique parfaite du signal causal de chaque incident.

La validation spécialisée devra utiliser :

```text
CPU_SATURATION
    -> cpu

DB_LATENCY
    -> latency-50 / latency-90

NETWORK_FAILURE
    -> socket / error
```

lorsque ces signaux sont disponibles.

Il faut donc distinguer :

```text
détection statistique d'une anomalie
```

de :

```text
identification du signal causal
```

---

## 21. `validate_detection`

Le détecteur fournit également une fonction de validation permettant de comparer les anomalies au ground truth.

Une détection valide vérifie notamment :

```text
anomaly.timestamp >= inject_time
```

et, si une fenêtre est définie :

```text
anomaly.timestamp <= inject_time + detection_window
```

Le résultat contient notamment :

```text
case_id
fault
incident_type
detected
first_detection_timestamp
detection_delay_seconds
anomaly_count
detection_method
root_cause_service
metadata
```

Cette structure est adaptée aux futures métriques d'évaluation.

---

## 22. Notebook de validation

Le notebook de cette étape est :

```text
notebooks/02_phase2_validation.ipynb
```

Il doit reproduire :

1. chargement du ground truth ;
2. sélection des cas ;
3. chargement des métriques ;
4. normalisation ;
5. construction des séries ;
6. calcul de baseline ;
7. détection ;
8. séparation avant/après injection ;
9. calcul du délai ;
10. validation des quatre faults ;
11. synthèse des résultats.

Le notebook constitue la preuve reproductible de la Phase 2.

---

## 23. Checklist

### Ingestion

- [x] `dataset_loader.py`
- [x] validation des chemins
- [x] chargement des 60 cas
- [x] chargement par cas
- [x] filtrage temporel
- [x] format long des métriques
- [x] `value` en `Float64`

### Normalisation

- [x] `schema_normalizer.py`
- [x] schéma canonique
- [x] métriques
- [x] logs
- [x] traces
- [x] extraction du service
- [x] extraction du signal
- [x] correction Polars 1.43.2
- [x] normalisation multi-sources

### Détection

- [x] `anomaly_detector.py`
- [x] construction des séries
- [x] baseline
- [x] exclusion de la période fautive
- [x] z-score
- [x] génération des anomalies
- [x] validation temporelle
- [x] CPU
- [x] DELAY
- [x] LOSS
- [x] SOCKET

### Documentation

- [x] commandes de validation
- [x] test d'intégration
- [x] résultats documentés
- [ ] exécution finale du notebook si elle reste à faire

---

## 24. Critères de fin

La Phase 2 est fonctionnellement validée lorsque :

```text
[x] Les données sont chargeables
[x] Les timestamps sont cohérents
[x] Les métriques sont numériques
[x] Les trois sources sont normalisées
[x] Les séries temporelles sont constructibles
[x] La baseline utilise uniquement la période normale
[x] Le z-score fonctionne
[x] Des anomalies sont générées
[x] Aucune anomalie n'est détectée avant injection sur les cas validés
[x] Les quatre incidents sont détectés
[x] Les résultats sont reproductibles
```

Le notebook doit être exécuté et conservé comme artefact expérimental de la phase.

---

## 25. Ce que la Phase 2 ne réalise pas

Cette phase ne réalise pas encore :

```text
RCA complète
corrélation multi-sources avancée
ranking des root causes
graphe dynamique complet du Digital Twin
raisonnement causal
architecture multi-agents
évaluation finale de la RCA
```

Le détecteur répond actuellement à :

> Quand une série métrique s'écarte-t-elle de son comportement normal ?

Il ne répond pas encore complètement à :

> Quel service est la root cause de l'incident ?

Pour cette question, il faudra combiner :

```text
anomalies
+
topologie
+
dépendances
+
logs
+
traces
+
temporalité
+
ground truth
```

---

## 26. Transition vers la suite

La Phase 1 avait préparé le dataset, le ground truth et la topologie. fileciteturn17file1L1-L25

La Phase 2 fournit maintenant la représentation temporelle et la première détection statistique.

La suite logique est :

```text
PHASE 1
Dataset & préparation
        |
        v
PHASE 2
Ingestion + Normalisation + Anomaly Detection
        |
        v
PHASE 3
Digital Twin / Topology / Correlation
        |
        v
PHASE 4
Root Cause Analysis
        |
        v
PHASE 5
Multi-Agent Architecture
        |
        v
PHASE 6
Evaluation
```

---

## 27. Conclusion

La Phase 2 a permis de passer d'un dataset préparé à une représentation exploitable par les composants intelligents d'ECDT.

Le pipeline fonctionnel est :

```text
RCAEval
   |
   v
DatasetLoader
   |
   v
Metrics / Logs / Traces
   |
   v
SchemaNormalizer
   |
   v
NormalizedEvent
   |
   v
TimeSeries
   |
   v
AnomalyDetector
   |
   v
AnomalyEvent
```

Sur les quatre cas testés :

```text
CPU
DELAY
LOSS
SOCKET
```

les anomalies apparaissent après l'injection et aucune anomalie n'a été observée avant l'injection.

Les résultats confirment donc que l'ingestion, la normalisation et la première détection statistique sont opérationnelles sur les cas validés.

Ils ne doivent toutefois pas être interprétés comme une RCA complète. L'identification robuste de la root cause nécessitera la combinaison de la détection avec la topologie, les dépendances, les traces, les logs et le ground truth.

**État : PHASE 2 — INGESTION + NORMALISATION + ANOMALY DETECTION : FONCTIONNELLE**
