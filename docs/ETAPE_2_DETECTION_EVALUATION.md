# Étape 2 — Ingestion, normalisation et détection

Ce paquet est un overlay à extraire à la racine du dépôt ECDT. Il a été
préparé à partir du commit `6838613` (`feat: harden pre-diagnostic observer
pipeline`). Il ne contient ni données RCAEval, ni résultats générés, ni
modification de l’agent Diagnostic/Impact.

## Effet des fichiers

| Fichier | Action | Rôle |
|---|---|---|
| `evaluation/phase2_evaluation.py` | remplacer | Répare le script Phase 2 et ajoute le pilote déterministe 12 cas |
| `scripts/run_observer_pipeline.py` | remplacer | Supprime une clé JSON dupliquée et nettoie le script sans changer l’algorithme |
| `src/ingestion/metric_quality.py` | ajouter | Explique chaque rejet métrique par motif, signal, métrique et service |
| `tests/test_metric_quality.py` | ajouter | Teste les cinq motifs et l’absence de rejet silencieux |
| `tests/test_anomaly_detector_edge_cases.py` | ajouter | Teste séries vides, courtes, constantes, NaN/infini et scores signés |
| `tests/test_phase2_evaluation.py` | ajouter | Teste les quatre profils sémantiques, l’isolation et le pilote 3 × 4 |

## Installation PowerShell

Depuis la racine du dépôt, vérifier d’abord que le commit précédent est propre :

```powershell
git status --short
```

Extraire ensuite le ZIP dans cette même racine en autorisant le remplacement
des deux fichiers existants. Puis vérifier la syntaxe :

```powershell
python -m py_compile `
  .\evaluation\phase2_evaluation.py `
  .\scripts\run_observer_pipeline.py `
  .\src\ingestion\metric_quality.py
```

## Tests rapides obligatoires

```powershell
python -m pytest `
  tests/test_metric_quality.py `
  tests/test_anomaly_detector_edge_cases.py `
  tests/test_phase2_evaluation.py `
  tests/test_observer_pipeline_reporting.py `
  -q
```

Puis exécuter les contrats déjà présents :

```powershell
python -m pytest `
  tests/test_ground_truth_isolation.py `
  tests/test_dataset_integrity.py `
  tests/test_dataset_manifest.py `
  -q
```

## Pilote déterministe de 12 cas

La commande suivante sélectionne automatiquement trois cas de chaque faute :
CPU, délai, perte et socket.

```powershell
python -m evaluation.phase2_evaluation `
  --project-root . `
  --output evaluation/results/phase2_pilot.json
```

Le rapport doit indiquer :

- `selected_cases: 12` et `failed_cases: 0` ;
- trois cas dans chaque entrée de `aggregate.by_fault` ;
- `all_metric_rejections_explained: true` ;
- `observer_score_contract_violations: 0` ;
- `reproducibility.identical: true` pour chaque cas ;
- une section `operational` sans `fault`, `root_cause_service` ni profil
  sémantique dérivé du type de faute ;
- les labels RCAEval uniquement dans `evaluation.ground_truth`.

Le champ `scope.mode` vaut `offline_benchmark`. L’instant d’injection est
utilisé comme frontière de baseline uniquement pour ce benchmark contrôlé ;
le rapport dit explicitement qu’il n’est pas supposé disponible en production.

## Campagne complète après validation du pilote

```powershell
python -m evaluation.phase2_evaluation `
  --project-root . `
  --all-cases `
  --output evaluation/results/phase2_60_cases.json
```

## Limites intentionnelles

- Le z-score, ses seuils et le mapping opérationnel signal → incident ne sont
  pas modifiés.
- Le détecteur statistique actuel consomme les métriques. Les logs et traces
  sont déclarés comme preuves contextuelles à étudier, pas comme entrées déjà
  prises en charge.
- Le découpage par `inject_time_ms` ne mesure pas les faux positifs avant
  injection, car cette même frontière empêche leur émission. Le rapport le
  signale au lieu de produire une précision trompeuse.
- Les résultats du pilote réel doivent être générés localement : les gros CSV
  `data/processed` ne sont pas versionnés dans Git et ne sont pas inclus ici.

## Commit conseillé après validation locale

```powershell
git add `
  evaluation/phase2_evaluation.py `
  scripts/run_observer_pipeline.py `
  src/ingestion/metric_quality.py `
  tests/test_metric_quality.py `
  tests/test_anomaly_detector_edge_cases.py `
  tests/test_phase2_evaluation.py `
  docs/ETAPE_2_DETECTION_EVALUATION.md

git diff --cached --check
git commit -m "feat: validate phase2 anomaly detection"
```
