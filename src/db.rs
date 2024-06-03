use std::path::Path;
use anyhow::{bail, Context};
use rusqlite::{Connection, OptionalExtension};

pub struct Database {
    sql: Connection,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
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
            name STRING PRIMARY KEY,\
            value STRING\
        )", [])?;

        Ok(Self { sql })
    }

    pub fn config(&self, name: &str) -> anyhow::Result<Option<String>> {
        self.sql.query_row(
            "SELECT value FROM config WHERE name = ?1",
            [name],
            |row| row.get(0)
        )
            .optional()
            .map_err(Into::into)
    }

    pub fn set_config(&self, name: &str, value: &str) -> anyhow::Result<()> {
        self.sql.execute("INSERT INTO config (name, value) VALUES (?1, ?2)", [name, value])?;
        Ok(())
    }
}
