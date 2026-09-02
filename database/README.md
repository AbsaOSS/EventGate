# EventGate Database

All database code lives here and is deployed with [Flyway](https://documentation.red-gate.com/flyway).
The migrations are the single source of truth for the schema, roles, and grants — the
same migrations build local, CI (integration tests), and real environments.

## Layout

```text
flyway.toml                            # Flyway configuration (locations, baseline, placeholders) — repo root
database/
├── README.md
└── migrations/
    ├── 00_databases.ddl               # One-off DB bootstrap (NOT a Flyway migration; no `V` prefix)
    ├── V1.4.0.1__create_roles.ddl     # owner / writer / reader roles
    ├── V1.4.0.2__initial_schema.ddl   # tables
    └── V1.4.0.3__grants.ddl           # ownership + least-privilege grants
    ...
```

## Conventions

- Versioned migrations follow Flyway's `V<major>.<minor>.<patch>.<step>__description.ext` format,
  where `<major>.<minor>.<patch>` tracks the EventGate release the migration ships in and `<step>`
  increments per migration within that release.
- Extensions carry intent: `.ddl` for structural changes (tables, roles, constraints, indexes),
  `.sql` for DML / data.

## Roles

| Role               | Purpose                                             | Used by             |
|--------------------|-----------------------------------------------------|---------------------|
| master (superuser) | Runs the migrations                                 | Flyway (deployment) |
| `eventgate_owner`  | Owns the schema objects, may run DDL                | Migrations          |
| `eventgate_writer` | `SELECT` / `INSERT` / `UPDATE` on data tables       | EventGate Lambda    |
| `eventgate_reader` | `SELECT` only                                       | EventStats Lambda   |

Role passwords are required Flyway placeholders (`eventgate_owner_password`,
`eventgate_writer_password`, `eventgate_reader_password`). Supply them from secrets in real
environments.

## Local setup

Requires the Flyway CLI (needs a JDK 17+) and Docker.

```zsh
# 1. Start a local Postgres docker container
docker run --name=eventgate_db -e POSTGRES_PASSWORD=changeme -e POSTGRES_DB=eventgate_db -p 5432:5432 -d postgres:16

# 2. Apply the migrations (run from the repo root, where flyway.toml lives)
export FLYWAY_PLACEHOLDERS_EVENTGATE_OWNER_PASSWORD=changeme
export FLYWAY_PLACEHOLDERS_EVENTGATE_WRITER_PASSWORD=changeme
export FLYWAY_PLACEHOLDERS_EVENTGATE_READER_PASSWORD=changeme
flyway migrate

# Inspect state / clean up
flyway info
docker kill eventgate_db && docker rm eventgate_db
```

## Adopting an existing database

On a database that already contains the tables but has no Flyway history (i.e. production), a
plain `flyway migrate` fails because Flyway sees existing objects it didn't create. The first
migration against such a database must instead pass baseline flags explicitly, one time only:

```zsh
flyway -baselineOnMigrate=true -baselineVersion=1.4.0.0 migrate
```

This records a baseline at `1.4.0.0` in `flyway_schema_history` and then applies `V1.4.0.1+` on
top.

Before the first production migration:

1. Compare the deployed schema with `V1.4.0.2__initial_schema.ddl`.
2. Back up the database and cluster roles.
3. Confirm the migration account can create roles and change ownership of every EventGate table.
4. Run `flyway info`, then the baseline command above with all role-password placeholders supplied from secrets.
