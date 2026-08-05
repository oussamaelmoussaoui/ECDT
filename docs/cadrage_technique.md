# Enterprise Cognitive Digital Twin (ECDT)

### Document de cadrage technique & business

| | |
|---|---|
| **Stage** | DXC Technology Morocco |
| **Nature du document** | Cadrage complet (business + technique) |
| **Périmètre couvert** | Vision, architecture cible, architecture PoC, stack technique, flux de données |

---

## Table des matières

1. [Résumé exécutif](#1-résumé-exécutif)
2. [Vision](#2-vision)
3. [Contexte business](#3-contexte-business)
4. [Énoncé du problème](#4-énoncé-du-problème)
5. [Solutions existantes & limites](#5-solutions-existantes--limites)
6. [Solution proposée](#6-solution-proposée)
7. [Objectifs du projet](#7-objectifs-du-projet)
8. [Cas d'usage en entreprise](#8-cas-dusage-en-entreprise)
9. [Proposition de valeur](#9-proposition-de-valeur)
10. [Exigences fonctionnelles](#10-exigences-fonctionnelles)
11. [Exigences non fonctionnelles](#11-exigences-non-fonctionnelles)
12. [Architecture Enterprise (cible)](#12-architecture-enterprise-cible)
13. [Architecture simplifiée (PoC)](#13-architecture-simplifiée-poc)
14. [Description détaillée des composants](#14-description-détaillée-des-composants)
15. [Stack technique détaillé](#15-stack-technique-détaillé)
16. [Flux de données](#16-flux-de-données)
17. [Diagrammes de séquence](#17-diagrammes-de-séquence)
18. [Évolutions futures](#18-évolutions-futures)
19. [Conclusion](#19-conclusion)

---

## 1. Résumé exécutif

L'**Enterprise Cognitive Digital Twin (ECDT)** est une plateforme qui construit une représentation numérique structurée et vivante d'une infrastructure IT, enrichie par une couche de raisonnement multi-agents capable :

- d'expliquer les incidents,
- de remonter aux causes racines,
- d'évaluer les impacts en cascade,
- et de proposer des actions correctives.

Contrairement aux outils de monitoring traditionnels, qui se limitent à afficher des métriques brutes et des alertes basées sur des seuils, l'ECDT raisonne sur les **relations** entre les composants de l'infrastructure pour répondre à la question que se pose réellement un opérateur pendant un incident :

> *« Pourquoi cela arrive-t-il, et qu'est-ce que cela va casser d'autre ? »*

Ce document regroupe le cadrage business, l'architecture cible complète, une architecture simplifiée de type **Proof of Concept (PoC)** réalisable dans les contraintes du stage (pas d'infrastructure client réelle, pas de déploiement en production, LLM gratuit uniquement), le stack technique détaillé, ainsi que le flux de données à travers le système.

## 2. Vision

Faire évoluer les opérations IT du **monitoring réactif** vers des **opérations cognitives et explicables**, où l'infrastructure n'est plus seulement observée, mais comprise.

La vision à long terme est celle d'une plateforme agissant comme un *« SRE junior »* permanent aux côtés des opérateurs humains : surveillant le système, corrélant les signaux entre services, et faisant remonter une analyse structurée et explicable pour permettre des décisions plus rapides et de meilleure qualité — **sans retirer l'humain de la boucle**.

## 3. Contexte business

Les environnements IT d'entreprise modernes sont hybrides et distribués : services cloud, clusters Kubernetes, machines virtuelles, bases de données et applications interconnectées génèrent en continu un flux de métriques, logs et événements.

Les outils d'observabilité (Prometheus, Grafana, Datadog, ELK, etc.) ont mûri autour de la **collecte et de la visualisation** de ces données, mais la responsabilité de **relier les points entre eux** pendant un incident repose encore presque entièrement sur les opérateurs humains.

Pour une entreprise de services IT comme DXC, opérant et supportant des infrastructures clients à grande échelle, cela se traduit par un coût concret :

- un **temps moyen de résolution (MTTR)** plus long,
- une **fatigue des opérateurs** due à la surcharge d'alertes,
- une **qualité de diagnostic inégale** selon l'expérience individuelle.

## 4. Énoncé du problème

> **Comment concevoir une plateforme capable de construire une représentation numérique vivante de l'infrastructure IT d'une entreprise, d'en comprendre les dépendances, d'analyser les incidents de manière autonome, et d'assister les équipes d'exploitation dans la prise de décision ?**

La difficulté principale n'est pas un manque de données — c'est un **manque de relations structurées entre les données**. Métriques, logs et traces existent en silos ; le graphe de dépendances reliant une base de données, les services qui l'utilisent et les utilisateurs qu'ils servent n'est capturé nulle part, si ce n'est dans la tête des ingénieurs.

## 5. Solutions existantes & limites

| Catégorie | Exemples | Limite |
|---|---|---|
| Monitoring de métriques | Prometheus, Grafana, Datadog | Montre *ce qui* a changé, pas *pourquoi*, ni ce que cela va affecter |
| Agrégation de logs | ELK Stack, Loki, Splunk | Nécessite une recherche/corrélation manuelle entre services pendant un incident |
| APM / Tracing | Jaeger, Dynatrace, New Relic | Trace bien les requêtes individuelles, mais ne raisonne pas sur les dépendances structurelles au niveau de l'infrastructure |
| AIOps traditionnel | Détection d'anomalies par seuils, runbooks statiques | Détecte les anomalies isolément ; la corrélation entre services est généralement manuelle ou basée sur des règles fragiles |
| CMDB | ServiceNow CMDB | Capture un inventaire majoritairement statique ; reflète rarement la topologie en temps réel et n'est pas exploité pour du raisonnement en direct |

**Constat commun à toutes ces catégories** : aucun de ces outils ne maintient à la fois un *modèle vivant et interrogeable des dépendances* et une *capacité de raisonnement* sur ce modèle. C'est précisément l'écart que l'ECDT vient combler.

## 6. Solution proposée

L'ECDT combine trois briques complémentaires :

1. **Un Knowledge Graph (le « Digital Twin »)** — une représentation structurée et interrogeable des ressources de l'infrastructure et des relations qui les unissent (dépendances, hébergement, communication).
2. **Une couche cognitive multi-agents** — des agents IA spécialisés qui observent, diagnostiquent, évaluent l'impact et recommandent des actions, coordonnés par un orchestrateur, combinant parcours de graphe (raisonnement structuré) et raisonnement par LLM (explication contextuelle en langage naturel).
3. **Une interface d'aide à la décision** — un dashboard qui traduit cette analyse en quelque chose qu'un opérateur peut lire, comprendre et exploiter en quelques secondes.

## 7. Objectifs du projet

- Construire un Knowledge Graph capable de représenter une topologie microservices réaliste et ses dépendances.
- Concevoir et implémenter un pipeline multi-agents capable de remonter un incident jusqu'à sa cause racine la plus probable.
- Évaluer l'impact en cascade d'un incident à partir du graphe de dépendances.
- Générer des explications et des actions recommandées en langage naturel via un LLM gratuit.
- Fournir un dashboard visuel et interactif permettant aux opérateurs d'explorer les incidents et le graphe sous-jacent.
- Mesurer objectivement la précision diagnostique du système face à des données d'incidents avec vérité terrain.

## 8. Cas d'usage en entreprise

| Cas d'usage | Description |
|---|---|
| **Accélération de la RCA** (Root Cause Analysis) | Réduire le temps passé à corréler manuellement les alertes entre services pendant un incident |
| **Prédiction de l'impact en cascade** | Anticiper quels services/utilisateurs seront affectés avant la propagation complète de l'incident |
| **Capitalisation de la connaissance des incidents** | Constituer une base croissante d'incidents passés et de leurs résolutions, consultable pour des cas futurs similaires |
| **Support à l'onboarding** | Les nouveaux opérateurs peuvent utiliser les explications de la plateforme pour comprendre plus vite des parties méconnues de l'infrastructure |
| **Reporting post-incident** | Générer automatiquement une explication structurée de ce qui s'est passé, utilisable comme base pour un post-mortem |

## 9. Proposition de valeur

- **Diagnostic plus rapide (réduction du MTTR)** — la corrélation entre signaux est automatisée plutôt que manuelle.
- **Visibilité unifiée** — un modèle cohérent de l'infrastructure plutôt que des dashboards dispersés.
- **Explicabilité** — chaque recommandation est appuyée par un chemin de raisonnement traçable dans le graphe, pas une boîte noire.
- **Anticipation proactive de l'impact** — estimation d'impact consciente des dépendances, avant propagation complète.
- **Apprentissage continu** — chaque incident résolu enrichit la base de connaissances pour les diagnostics futurs.

## 10. Exigences fonctionnelles

| # | Exigence | Statut PoC |
|---|---|---|
| FR-1 | Le système **doit** ingérer les métriques, logs et informations de topologie du système observé | ✅ Inclus |
| FR-2 | Le système **doit** construire et maintenir un Knowledge Graph représentant les ressources et leurs relations | ✅ Inclus |
| FR-3 | Le système **doit** détecter les anomalies à partir des signaux ingérés | ✅ Inclus |
| FR-4 | Le système **doit** identifier une cause racine probable pour un incident détecté, en s'appuyant sur le graphe | ✅ Inclus |
| FR-5 | Le système **doit** identifier les services en aval impactés par une cause racine donnée | ✅ Inclus |
| FR-6 | Le système **doit** générer une explication en langage naturel de l'incident | ✅ Inclus |
| FR-7 | Le système **doit** proposer une action de remédiation recommandée | ✅ Inclus |
| FR-8 | Le système **doit** fournir une interface visuelle pour explorer le graphe et l'historique des incidents | ✅ Inclus |
| FR-9 | Le système **doit** permettre la recherche d'incidents passés similaires via l'Agent Mémoire | ✅ Inclus |
| FR-10 | Le système **devrait** permettre à un opérateur de valider ou rejeter un diagnostic généré (boucle de feedback) | ⛔ Hors PoC — architecture cible |

## 11. Exigences non fonctionnelles

| Exigence | Détail |
|---|---|
| **Coût** | L'ensemble du stack repose sur des composants gratuits / open-source (pas de LLM payant, pas d'infrastructure cloud payante nécessaire pour le PoC) |
| **Reproductibilité** | L'environnement complet est déployable via Docker Compose sur une seule machine |
| **Explicabilité** | Chaque conclusion automatisée doit être traçable jusqu'au chemin du graphe et aux données qui l'ont produite — pas de sortie inexplicable de type boîte noire |
| **Modularité** | Chaque agent et chaque couche de l'architecture doit pouvoir être remplacé indépendamment (ex. changer de fournisseur LLM ne doit pas nécessiter de repenser les agents) |
| **Latence** | Une analyse d'incident (détection → explication) doit s'exécuter en quelques secondes à l'échelle du PoC |
| **Extensibilité** | L'architecture doit pouvoir monter en échelle vers une infrastructure réelle multi-cluster sans refonte fondamentale |

## 12. Architecture Enterprise (cible)

Il s'agit de l'architecture cible à long terme — la vision dont le PoC est une version réduite.

| Couche | Description |
|---|---|
| **1 — Infrastructure** | L'environnement IT réel (ou simulé) observé : clusters Kubernetes, machines virtuelles, bases de données, composants réseau et applications distribuées. Source de tous les signaux bruts |
| **2 — Collecte de données** | Collecte la télémétrie brute de la couche 1 : métriques (Prometheus), traces distribuées (OpenTelemetry) et logs (Loki/Elasticsearch). Normalise les signaux hétérogènes dans un schéma d'événement commun |
| **3 — Digital Twin** | Représentation numérique vivante de l'infrastructure : modèle synchronisé combinant état courant, données historiques en séries temporelles et topologie. « Source de vérité unique » du raisonnement cognitif |
| **4 — Knowledge Graph** | Implémentation structurée en base de graphe du Digital Twin : nœuds (`Service`, `Pod`, `Database`, `Node`, `Incident`) et arêtes (`DEPENDS_ON`, `RUNS_ON`, `COMMUNICATES_WITH`, `IMPACTS`, `CAUSED_BY`). Supporte les requêtes de parcours utilisées par la couche de raisonnement |
| **5 — Agents IA** | Agents autonomes spécialisés — Observateur, Diagnostic, Impact, Recommandation, Mémoire (voir détail ci-dessous) |
| **6 — Moteur de raisonnement** | Orchestration et raisonnement : gère l'état partagé entre agents, génère et exécute dynamiquement des requêtes Cypher, effectue du RAG sur logs/incidents passés, invoque le LLM pour la synthèse en langage naturel |
| **7 — Moteur de décision** | Consolide les sorties des agents en un objet de décision unique, priorisé et assorti d'un score de confiance : cause racine, ressources impactées, explication, action recommandée |
| **8 — Moteur d'exécution** | *(Architecture cible uniquement)* Déclenche optionnellement des actions de remédiation sûres et pré-approuvées à partir du Moteur de décision (ex. redémarrer un pod, scaler un service). **Hors périmètre du PoC** |
| **9 — Dashboard** | Interface humaine : visualisation du graphe, timeline des incidents, explications en langage naturel, actions recommandées, mécanisme de feedback opérateur (architecture cible) |

### Agents de la couche 5

- **Agent Observateur** — détecte et qualifie les anomalies.
- **Agent Diagnostic** — remonte la cause racine probable via le parcours du graphe + corrélation temporelle.
- **Agent Impact** — évalue les ressources affectées en aval.
- **Agent Recommandation** — propose une explication et une action de remédiation.
- **Agent Mémoire** — recherche des incidents passés similaires pour donner du contexte (également inclus dans le PoC).

### Diagramme — architecture complète

```
┌─────────────────────────────────────────────────────────────┐
│  Couche 1 — Infrastructure                                    │
│  (Kubernetes, VMs, BDD, réseau, applications distribuées)     │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 2 — Collecte de données                                │
│  (Prometheus, OpenTelemetry, Loki, normalisation des événements)│
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 3 — Digital Twin                                       │
│  (état synchronisé en direct + séries temporelles historiques) │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 4 — Knowledge Graph                                    │
│  (Neo4j : ressources, relations, structure de dépendances)     │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 5 — Agents IA                                          │
│  Observateur │ Diagnostic │ Impact │ Recommandation │ Mémoire  │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 6 — Moteur de raisonnement                             │
│  (orchestration, génération Cypher, RAG, appel au LLM)         │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 7 — Moteur de décision                                 │
│  (objet de décision consolidé, score de confiance)             │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 8 — Moteur d'exécution  (architecture cible uniquement)│
│  (actions de remédiation automatisées et pré-approuvées)       │
└───────────────────────────────┬───────────────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────┐
│  Couche 9 — Dashboard                                          │
│  (visualisation du graphe, timeline, explications, feedback)   │
└─────────────────────────────────────────────────────────────┘
```

## 13. Architecture simplifiée (PoC)

### Périmètre

Réduite à ce qui est réellement réalisable dans le cadre d'un stage, avec des outils gratuits et **sans infrastructure client réelle**. Le PoC repose sur une **exploitation directe d'un dataset public labellisé (RCAEval / CCF AIOPS)** — aucune infrastructure à déployer, aucune donnée propriétaire.

Par rapport à une première version ultra-réduite, ce périmètre réintègre trois éléments jugés importants pour la crédibilité du concept *« Cognitive Digital Twin »* :

- la **dimension Time Series** du Digital Twin (distincte du Knowledge Graph),
- un **RAG** sur les logs/incidents,
- un **Agent Mémoire** capitalisant sur les incidents passés.

Restent volontairement **hors périmètre** : l'auto-discovery dynamique, la boucle de feedback opérateur persistée, et le Moteur d'Exécution automatisé — trois éléments qui demandent beaucoup d'ingénierie pour peu de valeur démontrable dans un PoC.

### Correspondance avec l'architecture cible

| Couche de l'architecture complète | Équivalent PoC |
|---|---|
| Couche 1 — Infrastructure | Dataset public labellisé (RCAEval / CCF AIOPS) — exploitation directe, aucune infrastructure à déployer |
| Couche 2 — Collecte de données | Chargement des métriques/logs/traces du dataset, topologie extraite une fois (pas d'auto-discovery en direct) |
| Couche 3 — Digital Twin | **TimescaleDB** — historique des métriques en séries temporelles par ressource, utilisé pour la corrélation temporelle |
| Couche 4 — Knowledge Graph | **Neo4j** — structure des ressources et de leurs dépendances (services, bases de données, incidents) |
| Couche 5 — Agents IA | 4 agents : Observateur, Diagnostic+Impact (fusionnés), Recommandation, **Mémoire** |
| Couche 6 — Moteur de raisonnement | Orchestration séquentielle (LangGraph ou CrewAI), incluant un appel **RAG** (Chroma + embeddings locaux) avant la génération LLM |
| Couche 7 — Moteur de décision | Intégré dans la sortie de l'Agent Recommandation |
| Couche 8 — Moteur d'exécution | **Non inclus** — la remédiation reste une décision humaine |
| Couche 9 — Dashboard | Une seule page : graphe de dépendances + panneau d'incident avec explication, recommandation et incidents similaires |

### Diagramme simplifié

```
┌───────────────────────────────────────────────────┐
│  Dataset public labellisé (RCAEval / CCF AIOPS)    │
│  — exploitation directe, sans infrastructure client│
└───────────────────────┬─────────────────────────────┘
                         ▼
┌───────────────────────────────────────────────────┐
│  Chargement & normalisation                        │
│  (métriques, logs, traces, topologie)              │
└───────────────────────┬─────────────────────────────┘
                         ▼
              ┌──────────┴──────────┐
              ▼                     ▼
   ┌─────────────────────┐  ┌─────────────────────────┐
   │  Knowledge Graph     │  │  Digital Twin            │
   │  Neo4j                │  │  TimescaleDB (Time Series)│
   │  (structure/dépendances)│  │  (historique par ressource)│
   └───────────┬───────────┘  └────────────┬─────────────┘
               └───────────┬───────────────┘
                            ▼
┌───────────────────────────────────────────────────┐
│  Agents : Observateur → Diagnostic/Impact          │
│  (corrélation structurelle + temporelle)           │
└───────────────────────┬─────────────────────────────┘
                         ▼
┌───────────────────────────────────────────────────┐
│  RAG + Agent Mémoire                                │
│  Chroma (embeddings locaux, sentence-transformers)  │
│  — logs similaires + incidents passés similaires    │
└───────────────────────┬─────────────────────────────┘
                         ▼
┌───────────────────────────────────────────────────┐
│  Agent Recommandation                               │
│  (LLM Groq — explication + action suggérée)         │
└───────────────────────┬─────────────────────────────┘
                         ▼
┌───────────────────────────────────────────────────┐
│  Dashboard (graphe + timeline + explication +       │
│  incidents similaires)                              │
└───────────────────────────────────────────────────┘
```

### Scénario de démonstration

Un incident labellisé *« latence base de données »* est rejoué depuis le dataset :

1. L'**Agent Observateur** détecte l'anomalie sur la série temporelle correspondante (TimescaleDB) et crée le nœud `Incident` dans le graphe.
2. L'**Agent Diagnostic/Impact** combine le parcours du graphe (dépendances `PaymentService` → `OrderService` → `frontend`) avec la corrélation temporelle (quelle métrique a dévié en premier) pour confirmer la cause racine et l'impact.
3. Le **RAG** interroge Chroma pour retrouver des logs pertinents au moment de l'incident, et l'**Agent Mémoire** retrouve des incidents passés similaires.
4. L'**Agent Recommandation** combine tout ce contexte et génère :

   > *« Cause racine : latence de la base de données. Impact : PaymentService → OrderService → checkout. Incidents similaires : 2 cas passés avec la même signature. Action recommandée : vérifier le pool de connexions de la base de données. »*

Ce scénario, exécuté de bout en bout sur les 3 types d'incidents ciblés (latence DB, saturation CPU, panne réseau), constitue le **livrable central du PoC**, mesurable directement contre la vérité terrain du dataset.

## 14. Description détaillée des composants

| Composant | Fonction | Entrée | Sortie |
|---|---|---|---|
| **Dataset labellisé** (RCAEval / CCF AIOPS) | Fournit métriques, logs, traces et topologie d'un système de microservices réel, avec vérité terrain | Fichiers du dataset | Données brutes exploitables directement, sans infrastructure à déployer |
| **Loader de dataset** (Python) | Charge et normalise les fichiers du dataset vers un format d'événement commun | Fichiers bruts du dataset | Métriques/logs/traces/topologie normalisés |
| **Détecteur d'anomalies** (Python, seuils/z-score) | Signale les valeurs de métriques anormales sur les 3 types d'incidents ciblés | Séries temporelles normalisées | Événements d'anomalie |
| **Neo4j** | Stocke et interroge le Knowledge Graph (structure) | Topologie + événements d'incidents | Résultats de requêtes de parcours de graphe |
| **TimescaleDB** | Stocke l'historique des métriques en séries temporelles (Digital Twin) | Métriques normalisées, indexées par ressource et horodatage | Historique interrogeable pour la corrélation temporelle |
| **Chroma** | Base vectorielle pour le RAG et l'Agent Mémoire (deux collections distinctes) | Embeddings de logs et de résumés d'incidents passés | Résultats de recherche par similarité sémantique |
| **sentence-transformers** | Calcule les embeddings pour Chroma, sans dépendance API | Texte (logs, résumés d'incidents) | Vecteurs d'embedding |
| **Agent Observateur** | Détecte l'anomalie (via TimescaleDB) et crée le nœud Incident dans le graphe | Événements d'anomalie | Nœud d'incident structuré dans Neo4j |
| **Agent Diagnostic/Impact** | Remonte la cause racine et l'impact en combinant parcours du graphe et corrélation temporelle | Nœud d'incident + graphe + historique TimescaleDB | Cause racine candidate + ressources impactées |
| **Agent Mémoire** | Recherche des incidents passés similaires via Chroma | Résumé structuré de l'incident courant | Liste d'incidents similaires passés |
| **Agent Recommandation** | Génère une explication en langage naturel et une action suggérée, enrichie par le RAG | Cause racine + impact + contexte RAG + incidents similaires | Explication et recommandation lisibles par un humain |
| **Orchestrateur** (LangGraph/CrewAI) | Séquence l'exécution des 4 agents et partage l'état entre eux | Déclenchement d'incident | Analyse consolidée finale |
| **API Groq** | Héberge le LLM gratuit utilisé pour la génération en langage naturel | Prompt enrichi (graphe + time series + RAG + mémoire) | Texte en langage naturel |
| **Backend FastAPI** | Expose les données d'incidents, de graphe et d'historique via une API REST | Requêtes du frontend | Données JSON d'incidents/graphe/historique |
| **Dashboard Next.js/React** | Visualise le graphe, les incidents, les explications et les incidents similaires | Réponses de l'API | Interface utilisateur interactive |
| **Supabase/PostgreSQL** | Stocke l'historique applicatif des incidents traités | Incidents traités | Enregistrements persistés |

## 15. Stack technique détaillé

### AI

- **RAG (Retrieval-Augmented Generation)** — combine requêtes structurées sur le graphe, corrélation temporelle (TimescaleDB), et retrieval contextuel sur les logs (Chroma).
- **Chroma** *(gratuit, base vectorielle embarquée)* — deux collections distinctes : `logs` pour le RAG, `incidents_history` pour l'Agent Mémoire.
- **sentence-transformers** (`all-MiniLM-L6-v2`, gratuit, exécution locale sur CPU) — génère les embeddings sans dépendre d'une API payante ; Groq ne fournissant pas d'endpoint d'embeddings, cette brique est nécessaire pour un RAG 100 % gratuit.

### LLM

- **API Groq** *(offre gratuite)* — héberge des modèles rapides et open-weight (Llama 3, Mixtral) utilisés pour la génération d'explications et la synthèse du raisonnement.
- Architecture volontairement **agnostique du LLM** afin de pouvoir changer de fournisseur (OpenRouter, crédits gratuits Anthropic) sans repenser les agents.

### Backend

- **FastAPI** (Python) — couche API REST reliant le frontend au graphe, à la base de données et à l'orchestrateur d'agents.
- **Python** comme langage principal pour l'ingestion, le peuplement du graphe et la logique des agents — cohérent avec le profil data science du projet.

### Agents

- **LangGraph** *(ou CrewAI comme alternative plus rapide à prendre en main)* — orchestration multi-agents avec état partagé, adaptée à un flux de raisonnement en plusieurs étapes comme la RCA.
- 4 agents dans le PoC : Observateur, Diagnostic/Impact, **Mémoire**, Recommandation.

### Knowledge Graph

- **Neo4j Community Edition** *(gratuit)* — modélise la **structure** : ressources et dépendances (relations `DEPENDS_ON`, `RUNS_ON`, `IMPACTS`, `CAUSED_BY`).

### Digital Twin (Time Series)

- **TimescaleDB** *(extension PostgreSQL, gratuite)* — modélise l'**état dans le temps** : historique des métriques par ressource, requêtable pour la corrélation temporelle (quel signal a dévié en premier).
- Complète le Knowledge Graph plutôt que de s'y substituer : le graphe dit *qui dépend de qui*, TimescaleDB dit *ce qui s'est passé quand*.

### Bases de données

- **PostgreSQL / Supabase** *(offre gratuite)* — données applicatives, historique des incidents traités.
- **TimescaleDB** — séries temporelles (Digital Twin).
- **Chroma** — stockage vectoriel pour le RAG et l'Agent Mémoire.

### Monitoring

- Pas de collecte en direct (dataset rejoué plutôt que système en production) : les métriques/logs du dataset sont chargés directement dans TimescaleDB et Chroma, sans passer par Prometheus/Loki.

### Cloud

- Aucun cloud payant requis pour le PoC. Tout tourne en local via Docker Compose.
- Si une démo cloud est souhaitée plus tard, les offres gratuites de **Supabase**, **Neo4j Aura Free**, ou un programme de crédits cloud étudiant (GitHub Student Pack, etc.) permettent d'héberger le même stack sans coût.

### DevOps

- **Docker / Docker Compose** — orchestration locale du stack complet (Neo4j + TimescaleDB + Chroma + backend + frontend).
- **Git / GitHub** — gestion de version et (optionnellement) GitHub Actions pour la CI.

### Frontend

- **Next.js + React** — application dashboard.
- **react-force-graph** ou **Cytoscape.js** — visualisation interactive du graphe de dépendances.
- **Tailwind CSS** — style.

### Sécurité

- Aucune donnée sensible/client utilisée à aucun moment (environnement simulé ou dataset public uniquement) — supprime la majorité des enjeux de protection des données pour le PoC.
- Clés API (Groq, Supabase) stockées via des variables d'environnement, jamais commitées dans le dépôt.
- Modèle d'accès **en lecture seule** pour le dashboard dans le PoC — aucune écriture vers le système observé, ce qui supprime le risque que la plateforme elle-même provoque un incident.

### Justification synthétique des choix technologiques

| Technologie | Pourquoi ce choix |
|---|---|
| Neo4j | Adaptée nativement à la modélisation de dépendances/relations ; requêtes Cypher expressives pour le parcours RCA |
| TimescaleDB | Complète Neo4j pour la dimension Time Series du Digital Twin ; requêtable en SQL standard, gratuit |
| Chroma + sentence-transformers | RAG et Agent Mémoire 100 % gratuits et locaux, sans dépendre d'une API d'embeddings payante |
| Groq | Gratuit, inférence rapide, aucune barrière de coût pour un développement itératif |
| LangGraph/CrewAI | Conçus spécifiquement pour des workflows d'agents à plusieurs étapes avec état, tous deux gratuits et open-source |
| FastAPI | Léger, natif Python, s'intègre naturellement avec le code du graphe et des agents |
| Next.js/React | Cohérent avec les compétences frontend existantes, écosystème solide pour la visualisation de graphes |
| Docker Compose | Reproductibilité en une seule commande, aucune dépendance cloud requise |

## 16. Flux de données

1. Le dataset labellisé (RCAEval / CCF AIOPS) est chargé et normalisé : métriques vers **TimescaleDB**, topologie vers **Neo4j**, logs vers **Chroma** (collection `logs`, après calcul des embeddings via sentence-transformers).
2. Le détecteur d'anomalies évalue les séries temporelles stockées dans TimescaleDB par rapport à des seuils/lignes de base statistiques.
3. Lorsqu'une anomalie est détectée, un événement est émis et récupéré par l'**Agent Observateur**.
4. L'Agent Observateur crée un nœud `Incident` dans le Knowledge Graph Neo4j, relié à la ressource affectée.
5. L'**Agent Diagnostic/Impact** interroge le graphe (relations `DEPENDS_ON` en amont/aval) **et** TimescaleDB (quelle métrique a dévié en premier) pour confirmer la cause racine et lister les ressources impactées.
6. L'**Agent Mémoire** interroge Chroma (collection `incidents_history`) pour retrouver des incidents passés similaires par similarité sémantique.
7. Le **RAG** interroge Chroma (collection `logs`) pour retrouver les logs les plus pertinents au moment de l'incident.
8. L'**Agent Recommandation** envoie un prompt enrichi (cause racine, impact, chronologie, logs pertinents, incidents similaires) au LLM Groq, qui renvoie une explication en langage naturel et une action suggérée.
9. Le résultat consolidé est persisté (Supabase/PostgreSQL) — l'incident résumé et vectorisé est ajouté à `incidents_history` pour enrichir l'Agent Mémoire des futures analyses — puis exposé via le backend FastAPI.
10. Le dashboard Next.js récupère ces données et affiche l'incident, sa position dans le graphe de dépendances, l'explication générée et les incidents similaires retrouvés.

## 17. Diagrammes de séquence

> Les diagrammes de séquence détaillés (format image) sont disponibles dans `docs/architecture_diagrams/` :
>
> - **Séquence 1** — De la détection de l'incident à l'explication (Observateur → Diagnostic/Impact → RAG/Mémoire → Recommandation → persistance)
> - **Séquence 2** — Consultation du dashboard par l'opérateur (Frontend → API FastAPI → Neo4j/TimescaleDB/Supabase → rendu)
>
> *(Les images ont été extraites du document source d'origine et déplacées dans le dossier `docs/` pour garder ce fichier léger et lisible sur GitHub.)*

## 18. Évolutions futures

- **Auto-discovery dynamique** de la topologie de l'infrastructure, maintenant le Knowledge Graph continuellement synchronisé plutôt que chargé une seule fois.
- **Agent Mémoire** avec retrieval vectoriel sur les incidents historiques, permettant un raisonnement du type *« a-t-on déjà vu ce cas ? »*.
- **Boucle de feedback opérateur**, persistant la validation/le rejet des diagnostics pour améliorer progressivement les recommandations futures.
- **Moteur d'exécution**, permettant des actions de remédiation automatisées, sûres et pré-approuvées (avec des points de validation humaine).
- **Support multi-cluster / multi-environnement**, allant au-delà d'une topologie de démo unique vers une infrastructure d'entreprise réelle et distribuée.
- **Score de confiance et estimation d'incertitude** sur les prédictions de cause racine, pour aider les opérateurs à calibrer leur confiance dans les sorties du système.
- **Intégration avec les systèmes de ticketing** (ex. ServiceNow) pour créer ou enrichir automatiquement des tickets d'incident avec l'analyse générée.

## 19. Conclusion

Le projet Enterprise Cognitive Digital Twin propose un basculement du monitoring passif de l'infrastructure vers un raisonnement actif et explicable sur les opérations IT. En combinant un Knowledge Graph structuré avec une couche de raisonnement multi-agents coordonnée, la plateforme démontre — même à l'échelle d'un PoC — que l'analyse de cause racine et l'évaluation d'impact peuvent être automatisées de façon significative sans sacrifier l'explicabilité ni le contrôle humain.

L'architecture simplifiée définie dans ce document conserve intacte toute capacité de raisonnement essentielle de la vision complète, tout en restant réalisable avec des outils gratuits et open-source, et sans dépendance à une infrastructure client réelle — ce qui en fait un livrable réaliste et à forte valeur pour le stage, ainsi qu'une base solide pour une éventuelle évolution future vers une offre de niveau production.
