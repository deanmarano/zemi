---
title: 'Migration from Bemi'
sidebar_label: 'Migration Guide'
---

# Migration from Bemi

Zemi is a drop-in replacement for the original Bemi. This guide covers how to switch.

## What Changes

| Aspect | Original Bemi | Zemi |
|--------|--------------|------|
| Deployment | 3 processes in a ~3 GB Docker image | 1 static binary in a ~1 MB Docker image |
| Startup | 30–60 seconds | &lt;1 ms |
| Memory | 300–500+ MB | 2.8 MB |
| Docker image | `public.ecr.aws/bemi/dev:latest` | `ghcr.io/deanmarano/zemi:latest` |
| Default slot name | `bemi` | `zemi` |
| Default publication | `bemi` | `zemi` |

## What Stays the Same

- **`changes` table schema** — identical, existing queries work without modification
- **Environment variables** — same `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- **ORM packages** — all Bemi ORM packages (`bemi-prisma`, `bemi-rails`, `bemi-typeorm`, etc.) work without changes
- **Context stitching** — the `_bemi` protocol prefix is unchanged
- **Replication slot** — Zemi can reuse the same slot, resuming from the exact WAL position

## Migration Steps

### 1. Stop the Original Bemi

```bash
# If running via Docker:
docker stop bemi-worker

# If running via docker-compose:
docker-compose down
```

### 2. Note Your Current Configuration

Check your current slot and publication names:

```sql
-- Check replication slot
SELECT slot_name, confirmed_flush_lsn FROM pg_replication_slots;

-- Check publication
SELECT pubname FROM pg_publication;
```

### 3. Deploy Zemi

Set `SLOT_NAME` and `PUBLICATION_NAME` to match your existing values:

```bash
# If your original Bemi used default names:
docker run --rm \
  -e DB_HOST=your-db-host \
  -e DB_PORT=5432 \
  -e DB_NAME=your-db \
  -e DB_USER=postgres \
  -e DB_PASSWORD=secret \
  -e SLOT_NAME=bemi \
  -e PUBLICATION_NAME=bemi \
  ghcr.io/deanmarano/zemi:latest
```

Zemi will connect to the existing replication slot and resume processing from where Bemi left off. No data is lost or duplicated.

### 4. Verify

```sql
-- Check that the slot is active
SELECT slot_name, active, confirmed_flush_lsn FROM pg_replication_slots;

-- Check recent changes
SELECT id, "table", operation, created_at FROM changes ORDER BY created_at DESC LIMIT 5;
```

## Rollback

If you need to switch back to the original Bemi:

1. Stop Zemi
2. Start the original Bemi worker with the same configuration
3. It will resume from the same replication slot

The replication slot is shared between both systems. Only one can be connected at a time.

## Fresh Start

If you'd rather start fresh with Zemi's default configuration:

```bash
# Optionally clean up old Bemi resources
psql -c "SELECT pg_drop_replication_slot('bemi');" your-db
psql -c "DROP PUBLICATION bemi;" your-db

# Start Zemi with defaults (creates slot 'zemi' and publication 'zemi')
docker run --rm \
  -e DB_HOST=your-db-host \
  -e DB_NAME=your-db \
  -e DB_USER=postgres \
  -e DB_PASSWORD=secret \
  ghcr.io/deanmarano/zemi:latest
```

:::caution
Dropping the replication slot means Zemi will start from the current WAL position, not from where Bemi left off. Any changes that occurred between stopping Bemi and starting Zemi will not be captured.
:::

## ORM Package Compatibility

All Bemi ORM packages continue to work with Zemi without any changes:

| Package | Repository | Status |
|---------|-----------|--------|
| Prisma | [BemiHQ/bemi-prisma](https://github.com/BemiHQ/bemi-prisma) | Compatible |
| Ruby on Rails | [BemiHQ/bemi-rails](https://github.com/BemiHQ/bemi-rails) | Compatible |
| TypeORM | [BemiHQ/bemi-typeorm](https://github.com/BemiHQ/bemi-typeorm) | Compatible |
| SQLAlchemy | [BemiHQ/bemi-sqlalchemy](https://github.com/BemiHQ/bemi-sqlalchemy) | Compatible |
| Supabase JS | [BemiHQ/bemi-supabase-js](https://github.com/BemiHQ/bemi-supabase-js) | Compatible |
| MikroORM | [BemiHQ/bemi-mikro-orm](https://github.com/BemiHQ/bemi-mikro-orm) | Compatible |
| Django | [BemiHQ/bemi-django](https://github.com/BemiHQ/bemi-django) | Compatible |
| Drizzle | [BemiHQ/bemi-drizzle](https://github.com/BemiHQ/bemi-drizzle) | Compatible |

The packages use `pg_logical_emit_message` with the `_bemi` prefix to send context. Zemi detects and stitches this context identically to the original Bemi.
