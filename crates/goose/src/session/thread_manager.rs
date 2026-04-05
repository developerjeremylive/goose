use super::session_manager::{role_to_string, SessionStorage};
use crate::conversation::message::Message;
use anyhow::Result;
use chrono::{DateTime, Utc};
use rmcp::model::Role;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    pub id: String,
    pub name: String,
    pub user_set_name: bool,
    pub working_dir: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub archived_at: Option<DateTime<Utc>>,
    pub metadata: ThreadMetadata,
    #[serde(default)]
    pub current_session_id: Option<String>,
    #[serde(default)]
    pub message_count: i64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThreadMetadata {
    #[serde(default)]
    pub persona_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub provider_id: Option<String>,
    #[serde(default)]
    pub model_name: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

pub struct ThreadManager {
    storage: Arc<SessionStorage>,
}

const THREAD_SELECT: &str = "\
    SELECT t.id, t.name, t.user_set_name, t.working_dir, t.created_at, t.updated_at, \
    t.archived_at, t.metadata_json, \
    (SELECT s.id FROM sessions s WHERE s.thread_id = t.id ORDER BY s.created_at DESC LIMIT 1) as current_session_id, \
    (SELECT COUNT(*) FROM thread_messages WHERE thread_id = t.id) as message_count \
    FROM threads t";

type ThreadRow = (
    String,
    String,
    bool,
    Option<String>,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    i64,
);

fn thread_from_row(
    (
        id,
        name,
        user_set_name,
        working_dir,
        created_at,
        updated_at,
        archived_at_str,
        metadata_json,
        current_session_id,
        message_count,
    ): ThreadRow,
) -> Result<Thread> {
    let metadata: ThreadMetadata = serde_json::from_str(&metadata_json).unwrap_or_default();
    let archived_at = archived_at_str.as_deref().and_then(|s| s.parse().ok());
    Ok(Thread {
        id,
        name,
        user_set_name,
        working_dir,
        created_at: created_at.parse().unwrap_or_else(|_| Utc::now()),
        updated_at: updated_at.parse().unwrap_or_else(|_| Utc::now()),
        archived_at,
        metadata,
        current_session_id,
        message_count,
    })
}

impl ThreadManager {
    pub fn new(storage: Arc<SessionStorage>) -> Self {
        Self { storage }
    }

    pub async fn create_thread(
        &self,
        name: Option<String>,
        metadata: Option<ThreadMetadata>,
        working_dir: Option<String>,
    ) -> Result<Thread> {
        let pool = self.storage.pool().await?;
        let id = uuid::Uuid::new_v4().to_string();
        let name = name.unwrap_or_else(|| "New Chat".to_string());
        let meta = metadata.unwrap_or_default();
        let metadata_json = serde_json::to_string(&meta)?;

        sqlx::query(
            "INSERT INTO threads (id, name, user_set_name, working_dir, metadata_json) VALUES (?, ?, FALSE, ?, ?)",
        )
        .bind(&id)
        .bind(&name)
        .bind(&working_dir)
        .bind(&metadata_json)
        .execute(pool)
        .await?;

        self.get_thread(&id).await
    }

    pub async fn get_thread(&self, id: &str) -> Result<Thread> {
        let pool = self.storage.pool().await?;
        let sql = format!("{} WHERE t.id = ?", THREAD_SELECT);
        let row = sqlx::query_as::<_, ThreadRow>(&sql)
            .bind(id)
            .fetch_one(pool)
            .await?;

        thread_from_row(row)
    }

    pub async fn update_thread(
        &self,
        id: &str,
        name: Option<String>,
        user_set_name: Option<bool>,
        metadata: Option<ThreadMetadata>,
    ) -> Result<Thread> {
        let pool = self.storage.pool().await?;
        let mut sets = Vec::new();

        if name.is_some() {
            sets.push("name = ?");
            sets.push("user_set_name = ?");
        }
        if metadata.is_some() {
            sets.push("metadata_json = ?");
        }

        if !sets.is_empty() {
            let sql = format!(
                "UPDATE threads SET {}, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                sets.join(", ")
            );
            let mut q = sqlx::query(&sql);
            if let Some(ref n) = name {
                q = q.bind(n);
                q = q.bind(user_set_name.unwrap_or(true));
            }
            if let Some(ref meta) = metadata {
                q = q.bind(serde_json::to_string(meta)?);
            }
            q = q.bind(id);
            q.execute(pool).await?;
        }

        self.get_thread(id).await
    }

    pub async fn list_threads(&self, include_archived: bool) -> Result<Vec<Thread>> {
        let pool = self.storage.pool().await?;
        let sql = if include_archived {
            format!("{} ORDER BY t.updated_at DESC", THREAD_SELECT)
        } else {
            format!(
                "{} WHERE t.archived_at IS NULL ORDER BY t.updated_at DESC",
                THREAD_SELECT
            )
        };
        let rows = sqlx::query_as::<_, ThreadRow>(&sql).fetch_all(pool).await?;

        rows.into_iter().map(thread_from_row).collect()
    }

    pub async fn archive_thread(&self, id: &str) -> Result<Thread> {
        let pool = self.storage.pool().await?;
        sqlx::query("UPDATE threads SET archived_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE id = ?")
            .bind(id)
            .execute(pool)
            .await?;
        self.get_thread(id).await
    }

    pub async fn unarchive_thread(&self, id: &str) -> Result<Thread> {
        let pool = self.storage.pool().await?;
        sqlx::query(
            "UPDATE threads SET archived_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(id)
        .execute(pool)
        .await?;
        self.get_thread(id).await
    }

    pub async fn update_metadata(
        &self,
        id: &str,
        f: impl FnOnce(&mut ThreadMetadata),
    ) -> Result<Thread> {
        let thread = self.get_thread(id).await?;
        let mut meta = thread.metadata;
        f(&mut meta);
        self.update_thread(id, None, None, Some(meta)).await
    }

    pub async fn update_working_dir(&self, id: &str, working_dir: &str) -> Result<()> {
        let pool = self.storage.pool().await?;
        sqlx::query(
            "UPDATE threads SET working_dir = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        )
        .bind(working_dir)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn delete_thread(&self, id: &str) -> Result<()> {
        let pool = self.storage.pool().await?;
        let mut tx = pool.begin().await?;

        sqlx::query("DELETE FROM thread_messages WHERE thread_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "DELETE FROM messages WHERE session_id IN (SELECT id FROM sessions WHERE thread_id = ?)",
        )
        .bind(id)
        .execute(&mut *tx)
        .await?;
        sqlx::query("DELETE FROM sessions WHERE thread_id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM threads WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn append_message(
        &self,
        thread_id: &str,
        session_id: Option<&str>,
        message: &Message,
    ) -> Result<Message> {
        let pool = self.storage.pool().await?;
        let content_json = serde_json::to_string(&message.content)?;
        let metadata_json = serde_json::to_string(&message.metadata)?;
        let role_str = role_to_string(&message.role);

        let message_id = message
            .id
            .clone()
            .unwrap_or_else(|| format!("tmsg_{}", uuid::Uuid::new_v4()));

        sqlx::query(
            "INSERT INTO thread_messages (thread_id, session_id, message_id, role, content_json, created_timestamp, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(thread_id)
        .bind(session_id)
        .bind(&message_id)
        .bind(role_str)
        .bind(&content_json)
        .bind(message.created)
        .bind(&metadata_json)
        .execute(pool)
        .await?;

        sqlx::query("UPDATE threads SET updated_at = CURRENT_TIMESTAMP WHERE id = ?")
            .bind(thread_id)
            .execute(pool)
            .await?;

        let mut stored = message.clone();
        stored.id = Some(message_id);
        Ok(stored)
    }

    pub async fn list_messages(&self, thread_id: &str) -> Result<Vec<Message>> {
        let pool = self.storage.pool().await?;
        let rows = sqlx::query_as::<_, (Option<String>, String, Option<String>, String, i64, String)>(
            "SELECT message_id, role, session_id, content_json, created_timestamp, metadata_json FROM thread_messages WHERE thread_id = ? ORDER BY id ASC",
        )
        .bind(thread_id)
        .fetch_all(pool)
        .await?;

        let mut messages = Vec::new();
        for (message_id, role_str, _session_id, content_json, created_timestamp, metadata_json) in
            rows
        {
            let role = match role_str.as_str() {
                "user" => Role::User,
                "assistant" => Role::Assistant,
                _ => continue,
            };
            let content = serde_json::from_str(&content_json)?;
            let metadata = serde_json::from_str(&metadata_json).unwrap_or_default();

            let mut msg = Message::new(role, created_timestamp, content);
            msg.metadata = metadata;
            if let Some(id) = message_id {
                msg = msg.with_id(id);
            }
            messages.push(msg);
        }

        Ok(messages)
    }
}
