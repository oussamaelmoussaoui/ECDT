# Contribuer au projet

Merci de l'intérêt porté à **Enterprise Cognitive Digital Twin**. Ce document décrit comment mettre en place l'environnement de développement et les conventions à suivre pour contribuer au repo.

## Prérequis

- Docker & Docker Compose
- Python 3.11+
- Node.js 18+
- Une clé API Groq gratuite ([console.groq.com](https://console.groq.com))
- `make` (facultatif mais recommandé — voir le `Makefile`)

## Mise en place de l'environnement

```bash
git clone <url-du-repo>
cd ecdt-project
cp .env.example .env      # renseigner GROQ_API_KEY, NEO4J_*, TIMESCALE_*, etc.
make install               # installe les dépendances Python + Node
make up                    # démarre Neo4j, TimescaleDB, Chroma via Docker Compose
```

Voir le [README](README.md) pour le détail des variables d'environnement et de l'architecture.

## Convention de branches

- `main` — toujours stable, déployable.
- `feature/<nom-court>` — nouvelle fonctionnalité (ex. `feature/rag-retrieval`).
- `fix/<nom-court>` — correction de bug (ex. `fix/graph-query-timeout`).
- `docs/<nom-court>` — documentation uniquement.

## Convention de commits

On suit une variante simplifiée de [Conventional Commits](https://www.conventionalcommits.org/) :

```
<type>(<scope>): <description courte>

[corps optionnel]
```

**Types** : `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

**Exemples** :
```
feat(agents): ajouter l'Agent Mémoire avec retrieval Chroma
fix(knowledge-graph): corriger la requête Cypher de parcours amont
docs(readme): mettre à jour la section installation
test(rag): ajouter les tests du retriever de logs
```

## Style de code

**Python** (backend, agents, ingestion) :
- Formatage : `black`
- Linting : `ruff`
- Type hints recommandés sur les fonctions publiques

```bash
make lint     # black --check + ruff
make format   # applique le formatage automatiquement
```

**TypeScript / React** (frontend) :
- Formatage : `prettier`
- Linting : ESLint (config Next.js par défaut)

```bash
cd frontend && npm run lint
```

## Tests

Chaque nouvelle fonctionnalité touchant `src/` doit être accompagnée d'un test dans `tests/` correspondant :
- `src/knowledge_graph/` → `tests/test_graph_builder.py`
- `src/digital_twin/` → `tests/test_timeseries.py`
- `src/rag/` → `tests/test_rag.py`
- `src/memory/` → `tests/test_memory_agent.py`
- `src/agents/` → `tests/test_agents.py`
- `src/api/` → `tests/test_api.py`

```bash
make test
```

## Processus de Pull Request

1. Créer une branche depuis `main` selon la convention ci-dessus.
2. Faire des commits atomiques et clairs.
3. S'assurer que `make lint` et `make test` passent localement.
4. Ouvrir une PR avec une description : quoi, pourquoi, comment tester.
5. Lier la PR à l'étape correspondante de la [feuille de route](docs/etapes_realisation.md) si applicable.

## Documentation

Toute modification d'architecture (nouveau composant, changement de schéma du graphe, nouvel agent) doit être répercutée dans :
- `docs/cadrage_technique.md`
- Les diagrammes concernés dans `docs/architecture_diagrams/`

## Questions

Pour toute question sur le périmètre ou les choix techniques, se référer au document de cadrage ou ouvrir une issue avec le label `question`.
