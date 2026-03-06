---
title: Render - PostgreSQL Setup for Zemi
sidebar_label: Render
hide_title: true
description: How to configure your Render PostgreSQL database for Zemi change tracking using logical replication.
keywords: [Zemi, Render, PostgreSQL, Change Data Capture, logical replication, WAL]
---

# Render

## WAL level

Submit a Render support request to enable logical replication:

> In a few words, what can we help you with?

```
Configure database for logical replication
```

> Describe the issue in more detail.

```
- Set "wal_level" to "logical"
- Add "REPLICATION" permission to the database user
```

## Connection

Specify your database credentials from the Render dashboard:

* Use the full `Host` name ending with `.render.com` from the External Database URL section

![](/img/perm-render.png)

*Note: you can't create new credentials with `REPLICATION` permissions in Render.*
