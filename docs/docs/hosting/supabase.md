---
title: Supabase - PostgreSQL Setup for Zemi
sidebar_label: Supabase
hide_title: true
description: How to configure your Supabase PostgreSQL database for Zemi change tracking using logical replication.
keywords: [Zemi, Supabase, PostgreSQL, Change Data Capture, logical replication, WAL]
---

# Supabase

## WAL level

Supabase provisions PostgreSQL with the WAL level already set to `logical`. No changes needed.

## Connection

To connect Zemi to a [Supabase](https://supabase.com/) database, go to your Supabase project settings, untoggle "Use connection pooling", and use the direct connection details:

![](/img/perm-supabase.png)

*Note: you can't create new credentials with `REPLICATION` permissions in Supabase, see [this discussion](https://github.com/orgs/supabase/discussions/9314).*
