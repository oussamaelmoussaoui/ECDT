<div align="center">

# Enterprise Cognitive Digital Twin (ECDT)

**Plateforme multi-agents pour l'analyse cognitive des opérations IT**

*Stage de fin d'études — DXC Technology Morocco*

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Neo4j](https://img.shields.io/badge/Neo4j-Community-018BFF?logo=neo4j&logoColor=white)](https://neo4j.com/)
[![TimescaleDB](https://img.shields.io/badge/TimescaleDB-Time%20Series-FDB515?logo=postgresql&logoColor=black)](https://www.timescale.com/)
[![Next.js](https://img.shields.io/badge/Next.js-React-000000?logo=nextdotjs&logoColor=white)](https://nextjs.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-À%20définir-lightgrey)](#licence)

[Contexte](#contexte) • [Concept](#concept) • [Architecture](#architecture) • [Installation](#installation) • [Utilisation](#utilisation) • [Évaluation](#évaluation) • [Feuille de route](#feuille-de-route)

</div>

---

## Contexte

Les infrastructures IT modernes génèrent un volume massif de métriques, logs et événements — sans qu'aucun outil ne relie réellement ces signaux entre eux. Les équipes IT restent confrontées à :

- un **temps de diagnostic élevé** face à des incidents multi-composants,
- une **absence de vision unifiée** des dépendances entre ressources,
- une **corrélation d'alertes largement manuelle**, chronophage et sujette à l'erreur.

> **Problématique.** Comment construire une plateforme capable de comprendre les dépendances d'une infrastructure IT, d'analyser un incident de façon autonome, et d'assister les équipes dans leurs décisions ?

## Concept

L'**Enterprise Cognitive Digital Twin (ECDT)** répond à cette problématique en combinant trois briques complémentaires :

| Brique | Rôle |
|---|---|
| 🕸️ **Knowledge Graph** (Neo4j) | Représentation structurée des ressources IT et de leurs dépendances |
| ⏱️ **Digital Twin en Time Series** (TimescaleDB) | Historique des métriques par ressource, pour la corrélation temporelle |
| 🤖 **Couche cognitive multi-agents** | Observateur, Diagnostic/Impact, Mémoire (RAG), Recommandation — coordonnés par un orchestrateur combinant parcours de graphe, corrélation temporelle et raisonnement LLM |

### Exemple d'usage

> Une latence sur une base de données ralentit `PaymentService`, ce qui provoque des timeouts sur `OrderService` et des erreurs de checkout. Au lieu de trois alertes isolées, le système remonte **une explication consolidée** : cause racine confirmée par corrélation temporelle, impact en cascade, incidents similaires passés, et action recommandée.

Le projet acte un basculement du **monitoring passif** de l'infrastructure vers un **raisonnement actif et explicable**, sans sacrifier la maîtrise humaine du système : le PoC fonctionne en lecture seule sur l'infrastructure observée — aucune écriture n'est effectuée, ce qui élimine tout risque que la plateforme elle-même provoque un incident.

## Architecture

Version simplifiée retenue pour le PoC :

```
Dataset labellisé (RCAEval / CCF AIOPS)
              │
              ▼
  Chargement & normalisation
 (métriques, logs, traces, topologie)
              │
     ┌────────┴────────┐
     ▼                 ▼
 Knowledge Graph   Digital Twin
   (Neo4j)        (TimescaleDB —
                    Time Series)
     └────────┬────────┘
              ▼
   Agent Observateur → Agent Diagnostic/Impact
   (corrélation structurelle + temporelle)
              │
              ▼
   RAG + Agent Mémoire
   (Chroma — logs & incidents similaires)
              │
              ▼
   Agent Recommandation (LLM Groq)
              │
              ▼
   Dashboard (graphe + timeline + explication)
```

### Flux de données

1. Le dataset labellisé (RCAEval / CCF AIOPS) est chargé et normalisé : métriques → TimescaleDB, topologie → Neo4j, logs → Chroma (embeddings calculés via sentence-transformers).
2. Le détecteur d'anomalies évalue les séries temporelles stockées dans TimescaleDB par rapport à des seuils/lignes de base statistiques.
3. Une anomalie détectée émet un événement récupéré par l'**Agent Observateur**, qui crée un nœud `Incident` dans le Knowledge Graph, relié à la ressource affectée.
4. L'**Agent Diagnostic/Impact** interroge le graphe (relations `DEPENDS_ON` en amont/aval) et TimescaleDB (quelle métrique a dévié en premier) pour confirmer la cause racine et lister les ressources impactées.
5. L'**Agent Mémoire** interroge Chroma (collection `incidents_history`) pour retrouver des incidents passés similaires par similarité sémantique.
6. Le **RAG** interroge Chroma (collection `logs`) pour retrouver les logs les plus pertinents au moment de l'incident.
7. L'**Agent Recommandation** envoie un prompt enrichi (cause racine, impact, chronologie, logs pertinents, incidents similaires) au LLM Groq, qui renvoie une explication en langage naturel et une action suggérée.
8. Le résultat consolidé est persisté (Supabase/PostgreSQL) — l'incident résumé et vectorisé est ajouté à `incidents_history` pour enrichir l'Agent Mémoire — puis exposé via l'API FastAPI.
9. Le dashboard Next.js affiche l'incident, sa position dans le graphe de dépendances, l'explication générée et les incidents similaires retrouvés.

📄 Documentation complète : `docs/cadrage_technique.md` et `docs/architecture_diagrams/`.

## Stack technique

| Catégorie | Technologie | Pourquoi ce choix |
|---|---|---|
| Knowledge Graph | Neo4j Community Edition | Adapté nativement à la modélisation de dépendances/relations ; requêtes Cypher expressives pour le parcours RCA |
| Digital Twin (Time Series) | TimescaleDB | Complète Neo4j pour la dimension Time Series ; requêtable en SQL standard, gratuit |
| RAG & Agent Mémoire | Chroma + sentence-transformers | 100 % gratuit et local, sans dépendance à une API d'embeddings payante |
| LLM | Groq API (Llama / Mixtral) | Gratuit, inférence rapide, aucune barrière de coût pour un développement itératif |
| Orchestration multi-agents | LangGraph / CrewAI | Conçus pour des workflows d'agents à plusieurs étapes avec état, gratuits et open-source |
| Backend | FastAPI (Python) | Léger, natif Python, s'intègre naturellement au code du graphe et des agents |
| Frontend | Next.js + React, react-force-graph / Cytoscape.js | Cohérent avec les compétences existantes, écosystème solide pour la visualisation de graphes |
| Données applicatives | Supabase / PostgreSQL | — |
| Infrastructure | Docker Compose | Reproductibilité en une seule commande, aucune dépendance cloud requise |

> Aucune infrastructure cloud payante n'est requise pour exécuter ce PoC.

## Dataset

Le projet s'appuie sur l'exploitation directe d'un **dataset public labellisé** — **RCAEval** ou **CCF AIOPS Challenge** — sans aucune infrastructure client ni donnée propriétaire.

**Incidents cibles :**
- Latence base de données
- Saturation CPU
- Panne réseau

Chaque incident dispose d'une vérité terrain (root cause labellisée), utilisée pour l'évaluation objective du système.

## Structure du projet

```
ecdt-project/
├── data/                   # Dataset brut, traité, vérité terrain
├── notebooks/              # Exploration du dataset et de la topologie
├── src/
│   ├── ingestion/          # Chargement et normalisation du dataset
│   ├── knowledge_graph/    # Neo4j : schéma, peuplement, requêtes
│   ├── digital_twin/       # TimescaleDB : ingestion et requêtes Time Series
│   ├── rag/                # Chroma + embeddings, retrieval logs
│   ├── memory/             # Agent Mémoire (collection incidents_history)
│   ├── agents/             # Observateur, Diagnostic/Impact, Recommandation, orchestrateur
│   ├── llm/                # Client Groq
│   ├── api/                # Backend FastAPI
│   └── config/             # Configuration centralisée
├── frontend/                # Dashboard Next.js/React
├── evaluation/               # Scripts et rapport d'évaluation
├── tests/
└── docs/                     # Cadrage technique, diagrammes, étapes
```

## Installation

### Prérequis

- Docker & Docker Compose
- Python 3.11+
- Node.js 18+
- Une clé API Groq (gratuite) — [console.groq.com](https://console.groq.com)

### Étapes

```bash
git clone <url-du-repo>
cd ecdt-project

cp .env.example .env
# Renseigner GROQ_API_KEY, NEO4J_URI, TIMESCALE_URI, etc.

docker compose up -d               # Neo4j + TimescaleDB + Chroma

pip install -r requirements.txt --break-system-packages

cd frontend && npm install && cd ..
```

## Configuration

Variables d'environnement principales (`.env`) :

```env
GROQ_API_KEY=
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=
NEO4J_PASSWORD=
TIMESCALE_URI=postgresql://user:password@localhost:5432/ecdt
CHROMA_PERSIST_DIR=./chroma_data
SUPABASE_URL=
SUPABASE_KEY=
```

## Utilisation

```bash
# 1. Charger le dataset et peupler le Knowledge Graph + Digital Twin
python src/ingestion/dataset_loader.py
python src/knowledge_graph/graph_builder.py
python src/digital_twin/timeseries_ingestion.py

# 2. Indexer les logs pour le RAG
python src/rag/logs_indexer.py

# 3. Lancer le backend
uvicorn src.api.main:app --reload

# 4. Lancer le frontend
cd frontend && npm run dev
```

| Service | URL |
|---|---|
| Dashboard | `http://localhost:3000` |
| API | `http://localhost:8000` |

## Évaluation

```bash
python evaluation/run_evaluation.py
```

Génère un rapport (`evaluation/results/evaluation_report.md`) mesurant :

- le taux d'identification correcte de la cause racine contre la vérité terrain,
- la pertinence des ressources impactées identifiées,
- la pertinence du contenu retrouvé par le RAG et l'Agent Mémoire.

## Feuille de route

Voir `docs/etapes_realisation.md` pour le détail des 14 étapes :

`Cadrage → Dataset → Knowledge Graph → Digital Twin → Agents → RAG → Mémoire → Recommandation → API → Dashboard → Évaluation → Documentation`

### Évolutions futures envisagées

- **Auto-discovery dynamique** de la topologie de l'infrastructure, maintenant le Knowledge Graph continuellement synchronisé plutôt que chargé une seule fois
- **Boucle de feedback opérateur** persistée, pour améliorer progressivement les recommandations
- **Moteur d'exécution automatisé** permettant des actions de remédiation sûres et pré-approuvées, avec points de validation humaine
- **Support multi-cluster / multi-environnement**, au-delà d'une topologie de démo unique
- **Score de confiance / estimation d'incertitude** sur les prédictions de cause racine
- **Intégration avec les systèmes de ticketing** (ex. ServiceNow) pour créer ou enrichir automatiquement des tickets d'incident

## Limites connues

Hors périmètre de ce PoC (pistes d'évolution future) :

- Auto-discovery dynamique de la topologie
- Boucle de feedback opérateur persistée
- Moteur d'exécution automatisé (remédiation)
- Support multi-cluster / multi-environnement

## Conclusion

Le projet ECDT propose un basculement du monitoring passif de l'infrastructure vers un raisonnement actif et explicable sur les opérations IT. En combinant un Knowledge Graph structuré avec une couche de raisonnement multi-agents coordonnée, la plateforme démontre — même à l'échelle d'un PoC — que l'analyse de cause racine et l'évaluation d'impact peuvent être automatisées de façon significative sans sacrifier l'explicabilité ni le contrôle humain.

L'architecture simplifiée conserve intacte toute capacité de raisonnement essentielle de la vision complète, tout en restant réalisable avec des outils gratuits et open-source, sans dépendance à une infrastructure client réelle.

## Licence

Projet réalisé dans le cadre d'un stage — DXC Technology Morocco. *[À compléter selon la politique de l'entreprise.]*

---

<div align="center">

*Développé dans le cadre d'un stage de fin d'études — DXC Technology Morocco*

</div>
