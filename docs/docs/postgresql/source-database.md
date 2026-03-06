---
title: Source Database Setup
sidebar_label: Source Database
hide_title: true
description: How to configure your PostgreSQL source database for Zemi change tracking using logical replication (WAL).
keywords: [PostgreSQL, Change Data Capture, Zemi, logical replication, WAL, wal_level]
---

# Source Database

Zemi tracks changes made in a PostgreSQL database by connecting to the built-in [Write-Ahead Log](https://www.postgresql.org/docs/current/wal-intro.html) (WAL) using logical replication.

## WAL Level

Zemi requires `wal_level = logical`, which enables row-level change streaming (as opposed to physical replication which sends disk block changes).

Check your current setting:

```sql
SHOW wal_level;
+-------------+
| wal_level   |
|-------------|
| logical     |
+-------------+
```

If it shows `replica`, update it and restart PostgreSQL:

```sql
ALTER SYSTEM SET wal_level = logical;
-- Restart PostgreSQL after this change
```

:::note
Changing from `replica` to `logical` won't break existing replication. It slightly increases WAL volume (disk space and network traffic for replicas).
:::

Platform-specific instructions for enabling logical replication:

* **[Supabase](/hosting/supabase)**
* **[Neon](/hosting/neon)**
* **[AWS RDS](/hosting/aws)**
* **[GCP Cloud SQL](/hosting/gcp)**
* **[Render](/hosting/render)**
* **[DigitalOcean](/hosting/digitalocean)**
* **[Self-Managed](/hosting/self-managed)**

## Connection

Zemi connects using standard PostgreSQL credentials:

```bash
DB_HOST=your-db-host \
DB_PORT=5432 \
DB_NAME=your-db \
DB_USER=postgres \
DB_PASSWORD=secret \
./zig-out/bin/zemi
```

For cloud databases that require SSL, add:

```bash
DB_SSL_MODE=verify-full
```

See the [Configuration Reference](../zemi/configuration) for all available options.

## Table Tracking

### Tracking Specific Tables

Use the `TABLES` environment variable to track only specific tables:

```bash
TABLES=users,orders,payments ./zig-out/bin/zemi
```

When `TABLES` is set, Zemi creates a publication scoped to those tables. When unset, it creates a `FOR ALL TABLES` publication.

### REPLICA IDENTITY FULL

To track both "before" and "after" states on UPDATE and DELETE operations, set `REPLICA IDENTITY FULL` on tracked tables:

```sql
ALTER TABLE users REPLICA IDENTITY FULL;
ALTER TABLE orders REPLICA IDENTITY FULL;
```

Without this, UPDATE changes will only include the new values (no "before" state), and DELETE changes will only include the primary key.

## Read-Only Credentials

For production use, you can create a dedicated read-only user for Zemi:

```sql
-- Create a user with replication permission
CREATE ROLE zemi_user WITH LOGIN REPLICATION PASSWORD 'your-password';

-- Grant SELECT access for selective tracking
GRANT SELECT ON ALL TABLES IN SCHEMA public TO zemi_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO zemi_user;
```

On AWS RDS, use `GRANT rds_replication TO zemi_user;` instead of the `REPLICATION` attribute.

## Disconnecting

To stop Zemi and clean up replication resources:

```sql
-- Drop the replication slot (default name: 'zemi')
SELECT pg_drop_replication_slot('zemi');

-- Drop the publication (default name: 'zemi')
DROP PUBLICATION zemi;
```

Or use `CLEANUP_ON_SHUTDOWN=true` to have Zemi automatically clean up on graceful shutdown. See [Configuration](../zemi/configuration) for details.

:::warning
Dropping the replication slot means any WAL changes that occur before Zemi restarts will not be captured. Only do this if you don't need continuous tracking across restarts.
:::
