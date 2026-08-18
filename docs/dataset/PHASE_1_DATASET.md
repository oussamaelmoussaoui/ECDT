# PHASE 1 — DATASET

## Dataset

- Source : RCAEval
- Nombre total de cas : 735
- Doublons : 0
- Datasets : 9
- Root causes : 18
- Fault types : 11

## Scénarios ECDT validés

### CPU
- RCAEval fault : cpu
- Nombre de cas : 120
- Interprétation : CPU stress / saturation

### Database Latency
- RCAEval fault : delay
- Nombre de cas : 120
- Interprétation : network delay utilisé comme scénario de latence

### Network Failure
- RCAEval faults : loss + socket
- LOSS : 120 cas
- SOCKET : 45 cas

## Télémétrie

- Logs disponibles : 359 / 735
- Traces disponibles : 240 / 735

## Sous-ensemble de référence

- 60 cas
- RE2-OB : 20
- RE2-SS : 20
- RE2-TT : 20
- CPU : 15
- DELAY : 15
- LOSS : 15
- SOCKET : 15

## Topologie

Une première topologie de référence a été extraite depuis les traces
du cas RE2-OB checkoutservice delay.

Services observés :

- checkoutservice
- currencyservice
- emailservice
- frontendservice
- paymentservice
- productcatalogservice
- recommendationservice

## Statut

PHASE 1 — DATASET : TERMINÉE