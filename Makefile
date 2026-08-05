.DEFAULT_GOAL := help

PYTHON := python3
PIP := pip
COMPOSE := docker compose

.PHONY: help install install-python install-frontend up down logs \
        ingest build-graph build-twin index-rag \
        api frontend dev \
        lint format test evaluate \
        clean clean-data

## ── Aide ──────────────────────────────────────────────────

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

## ── Installation ─────────────────────────────────────────

install: install-python install-frontend ## Installe toutes les dépendances (Python + Node)

install-python: ## Installe les dépendances Python
	$(PIP) install -r requirements.txt --break-system-packages

install-frontend: ## Installe les dépendances du frontend
	cd frontend && npm install

## ── Infrastructure ──────────────────────────────────────

up: ## Démarre Neo4j, TimescaleDB, Chroma via Docker Compose
	$(COMPOSE) up -d

down: ## Arrête les conteneurs Docker
	$(COMPOSE) down

logs: ## Affiche les logs des conteneurs
	$(COMPOSE) logs -f

## ── Pipeline de données ─────────────────────────────────

ingest: ## Charge et normalise le dataset (RCAEval / CCF AIOPS)
	$(PYTHON) src/ingestion/dataset_loader.py

build-graph: ## Construit le Knowledge Graph (Neo4j)
	$(PYTHON) src/knowledge_graph/graph_builder.py

build-twin: ## Peuple le Digital Twin en Time Series (TimescaleDB)
	$(PYTHON) src/digital_twin/timeseries_ingestion.py

index-rag: ## Indexe les logs pour le RAG (Chroma + embeddings)
	$(PYTHON) src/rag/logs_indexer.py

pipeline: ingest build-graph build-twin index-rag ## Exécute tout le pipeline de données (ingestion → graph → twin → RAG)

## ── Développement ────────────────────────────────────────

api: ## Lance le backend FastAPI (mode dev)
	uvicorn src.api.main:app --reload

frontend: ## Lance le frontend Next.js (mode dev)
	cd frontend && npm run dev

dev: up ## Démarre l'infra puis affiche les commandes à lancer (api / frontend dans des terminaux séparés)
	@echo "Infrastructure démarrée. Lancez 'make api' et 'make frontend' dans deux terminaux séparés."

## ── Qualité de code ──────────────────────────────────────

lint: ## Vérifie le style de code Python (black + ruff)
	black --check src tests
	ruff check src tests

format: ## Formate automatiquement le code Python
	black src tests
	ruff check --fix src tests

test: ## Lance les tests Python
	pytest tests/ -v

## ── Évaluation ───────────────────────────────────────────

evaluate: ## Exécute l'évaluation du système sur le jeu de test labellisé
	$(PYTHON) evaluation/run_evaluation.py

## ── Nettoyage ────────────────────────────────────────────

clean: ## Supprime les fichiers Python temporaires (cache, pyc...)
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-data: down ## Arrête les conteneurs et supprime les volumes de données locaux
	rm -rf neo4j_data timescale_data chroma_data
