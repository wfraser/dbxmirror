use std::convert::identity;
use std::path::Path;
use anyhow::{anyhow, bail, Context};
use rusqlite::{Connection, DatabaseName, OptionalExtension, params};
use crate::CommonOptions;

pub struct Database {
    sql: Connection,
}

impl Database {
    pub fn open(path: impl AsRef<Path>, opts: &CommonOptions) -> anyhow::Result<Self> {
        let sql = Connection::open(path.as_ref())
            .with_context(|| format!("failed to open {:?}", path.as_ref()))?;

        sql.execute("CREATE TABLE IF NOT EXISTS meta (\
            id INTEGER PRIMARY KEY,\
            schema INTEGER\
        )", [])?;

        sql.execute("INSERT INTO meta (id, schema) VALUES (1, 1) ON CONFLICT DO NOTHING", [])?;

        let schema: i32 = sql.query_row("SELECT schema FROM meta WHERE id = 1", [], |row| row.get(0))?;

        if schema != 1 {
            bail!("unrecognized schema version {schema}");
        }

        sql.execute("CREATE TABLE IF NOT EXISTS config (\
            name TEXT PRIMARY KEY,\
            value TEXT\
        )", [])?;

        sql.execute("CREATE TABLE IF NOT EXISTS files (\
            path TEXT PRIMARY KEY,\
            mtime INTEGER,\
            content_hash TEXT\
        )", [])?;

        sql.execute("CREATE TABLE IF NOT EXISTS ignores (\
            path TEXT PRIMARY KEY,\
            regex INTEGER\
        )", [])?;

        if opts.turbo {
            sql.pragma_update(Some(DatabaseName::Main), "synchronous", "OFF")?;
        }

        Ok(Self { sql })
    }

    pub fn config_opt(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.sql.query_row(
            "SELECT value FROM config WHERE name = ?1",
            [name],
            |row| row.get(0)
        )
            .optional()
            .map_err(Into::into)
    }

    pub fn config(&self, name: &str) -> anyhow::Result<String> {
        self.config_opt(name)
            .transpose()
            .ok_or_else(|| anyhow!("missing config value {name:?}"))
            .and_then(identity)
    }

    pub fn set_config(&self, name: &str, value: &str) -> anyhow::Result<()> {
        self.sql.execute("INSERT INTO config (name, value) VALUES (?1, ?2) \
            ON CONFLICT DO UPDATE SET value = ?2 where name = ?1",
     [name, value])
            .with_context(|| name.to_owned())?;
        Ok(())
    }

    pub fn unset_config(&self, name: &str) -> anyhow::Result<()> {
        self.sql.execute("DELETE FROM config WHERE name = ?1", [name])?;
        Ok(())
    }

    pub fn set_file(&self, path: &str, mtime: i64, content_hash: &str) -> anyhow::Result<()> {
        self.sql.execute(
            "INSERT INTO files (path, mtime, content_hash) VALUES (?1, ?2, ?3) \
                ON CONFLICT DO UPDATE SET mtime = ?2, content_hash = ?3 WHERE path = ?1",
            rusqlite::params![path, mtime, content_hash],
        )?;
        Ok(())
    }

    pub fn get_file(&self, path: &str) -> anyhow::Result<Option<(i64, String)>> {
        if let Some((mtime, content_hash)) = self.sql.query_row(
            "SELECT mtime, content_hash FROM files WHERE path = ?1",
            [path],
            |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()?
        {
            Ok(Some((mtime, content_hash)))
        } else {
            Ok(None)
        }
    }

    pub fn for_files(&self, mut f: impl FnMut(&str) -> anyhow::Result<()>) -> anyhow::Result<()> {
        self.sql.prepare("SELECT path FROM files")?
            .query_map([], |row| row.get(0))?
            .try_for_each(|r: rusqlite::Result<String>| {
                let path = r?;
                f(&path)
            })
    }

    pub fn remove_file(&self, path: &str) -> anyhow::Result<()> {
        self.sql.execute("DELETE FROM files WHERE path = ?1", [path])?;
        Ok(())
    }

    pub fn add_ignore(&self, path: &str, regex: bool) -> anyhow::Result<()> {
        self.sql.execute("INSERT INTO ignores (path, regex) VALUES (?1, ?2)", params![path, regex])?;
        Ok(())
    }

    pub fn remove_ignore(&self, path: &str) -> anyhow::Result<()> {
        self.sql.execute("DELETE FROM ignores WHERE path = ?1", [path])?;
        if self.sql.changes() == 0 {
            bail!("no matching ignore rule");
        }
        Ok(())
    }

    pub fn ignores(&self) -> anyhow::Result<Vec<(String, bool)>> {
        let v = self.sql.prepare("SELECT path, regex FROM ignores ORDER BY path ASC")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(v)
    }
}
