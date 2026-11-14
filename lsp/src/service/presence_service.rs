/*
 * This file is part of discord-presence. Extension for Zed that adds support for Discord Rich Presence using LSP.
 *
 * Copyright (c) 2024 Steinhübl
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

use crate::{
    activity::ActivityManager,
    config::PresenceConfig,
    document::Document,
    error::Result,
    idle::IdleManager,
    service::{AppState, FileMonitor},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, warn};

/// How often each instance checks whether it still owns the presence.
const OWNERSHIP_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Remembers the previous ownership check to detect the moment it is lost.
#[derive(Debug)]
struct OwnershipWatch {
    was_owner: bool,
}

impl OwnershipWatch {
    fn new(is_owner: bool) -> Self {
        Self {
            was_owner: is_owner,
        }
    }

    /// Records a check; returns true only on an owner -> non-owner transition.
    fn lost_ownership(&mut self, is_owner: bool) -> bool {
        let lost = self.was_owner && !is_owner;
        self.was_owner = is_owner;
        lost
    }

    /// Ownership came back before the release ran: keep watching as owner, so
    /// the next loss is detected again.
    fn mark_owner(&mut self) {
        self.was_owner = true;
    }
}

#[derive(Debug)]
pub struct PresenceService {
    state: Arc<AppState>,
    idle_manager: Arc<IdleManager>,
    file_monitor: FileMonitor,
    debounce_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    pending_doc: Arc<Mutex<Option<Document>>>,
    update_debounce: Duration,
}

impl PresenceService {
    pub fn new(state: Arc<AppState>, config: PresenceConfig) -> Self {
        let idle_manager = Arc::new(IdleManager::new(
            Arc::clone(&state.shutting_down),
            Arc::clone(&state.owner),
        ));

        Self {
            state,
            idle_manager,
            file_monitor: FileMonitor::new(),
            debounce_handle: Arc::new(Mutex::new(None)),
            pending_doc: Arc::new(Mutex::new(None)),
            update_debounce: config.update_debounce,
        }
    }

    pub fn file_monitor(&self) -> &FileMonitor {
        &self.file_monitor
    }

    pub async fn update_presence(&self, doc: Option<Document>) -> Result<()> {
        // Store the document for deferred update
        *self.pending_doc.lock().await = doc;

        // Cancel any pending debounce timer
        if let Some(handle) = self.debounce_handle.lock().await.take() {
            handle.abort();
        }

        let state = Arc::clone(&self.state);
        let pending_doc = Arc::clone(&self.pending_doc);
        let debounce_handle = Arc::clone(&self.debounce_handle);
        let debounce_delay = self.update_debounce;
        let idle_manager = Arc::clone(&self.idle_manager);

        // Spawn debounce task
        let task = tokio::spawn(async move {
            tokio::time::sleep(debounce_delay).await;

            // Get the latest pending doc (might have changed during debounce).
            // Take it in its own statement: in edition 2021 a lock guard in an
            // `if let` scrutinee lives until the end of the block, which made
            // every update_presence call wait for the whole Discord send.
            let doc = pending_doc.lock().await.take();
            if let Some(doc) = doc {
                let update_start = std::time::Instant::now();
                if let Err(e) =
                    Self::perform_update_internal(&state, &idle_manager, Some(doc)).await
                {
                    warn!("Failed to update Discord presence: {}", e);
                } else {
                    let total_elapsed = update_start.elapsed();
                    info!(
                        "Discord presence updated in {:.1}ms (debounce delay: {:.0}ms)",
                        total_elapsed.as_secs_f64() * 1000.0,
                        debounce_delay.as_secs_f64() * 1000.0
                    );
                }
            }

            // Clear the handle
            *debounce_handle.lock().await = None;
        });

        *self.debounce_handle.lock().await = Some(task);
        Ok(())
    }

    async fn perform_update_internal(
        state: &Arc<AppState>,
        idle_manager: &IdleManager,
        doc: Option<Document>,
    ) -> Result<()> {
        if state.is_shutting_down() {
            debug!("Skipping presence update because shutdown is in progress");
            return Ok(());
        }

        // Store the last document for idle use
        {
            let mut last_doc = state.last_document.lock().await;
            (*last_doc).clone_from(&doc);
        }

        // Reset idle timeout if document changed. The shared manager aborts the
        // previous timer, so only the most recent update can trigger the idle action.
        if doc.is_some() {
            let workspace_name = {
                let workspace = state.workspace.lock().await;
                workspace.name().to_string()
            };
            idle_manager
                .reset_timeout(
                    Arc::clone(&state.discord),
                    Arc::clone(&state.config),
                    Arc::clone(&state.git_remote_url),
                    Arc::clone(&state.git_branch),
                    Arc::clone(&state.last_document),
                    workspace_name,
                )
                .await;
        }

        if state.is_shutting_down() {
            debug!("Skipping Discord activity update because shutdown started mid-update");
            return Ok(());
        }

        if !state.owner.is_owner() {
            debug!("Another instance owns the presence, skipping Discord activity update");
            return Ok(());
        }

        // Build and set activity
        let activity_fields = Self::build_activity_fields_internal(state, doc.as_ref()).await?;
        let git_url = Self::get_git_url_if_enabled_internal(state).await?;

        // Set Discord activity
        if state.is_shutting_down() {
            debug!("Skipping Discord activity update because shutdown is in progress");
            return Ok(());
        }

        let mut discord = state.discord.lock().await;

        discord
            .change_activity_with_reconnect(activity_fields, git_url)
            .await?;

        Ok(())
    }

    async fn build_activity_fields_internal(
        state: &Arc<AppState>,
        doc: Option<&Document>,
    ) -> Result<crate::activity::ActivityFields> {
        let config = state.config.lock().await;
        let workspace = state.workspace.lock().await;
        let git_branch = state.git_branch.lock().await.clone();
        let git_remote_url = state.git_remote_url.lock().await.clone();

        Ok(ActivityManager::build_activity_fields(
            doc,
            &config,
            workspace.name(),
            workspace.path().unwrap_or(""),
            git_branch,
            git_remote_url.as_deref(),
        ))
    }

    async fn get_git_url_if_enabled_internal(state: &Arc<AppState>) -> Result<Option<String>> {
        let config = state.config.lock().await;
        let git_remote_url = state.git_remote_url.lock().await.clone();

        let workspace_override = {
            let workspace = state.workspace.lock().await;
            config
                .find_workspace_override(workspace.path().unwrap_or(""), git_remote_url.as_deref())
                .map(|ov| ov.effective_git_integration(config.git_integration))
        };

        let git_enabled = workspace_override.unwrap_or(config.git_integration);

        if git_enabled {
            Ok(git_remote_url)
        } else {
            Ok(None)
        }
    }

    pub async fn initialize_discord(&self, application_id: &str) -> Result<()> {
        if self.state.is_shutting_down() {
            debug!("Skipping Discord initialization because shutdown is in progress");
            return Ok(());
        }

        let mut discord = self.state.discord.lock().await;
        discord.create_client(application_id)?;
        discord.connect_with_retry().await?;
        Ok(())
    }

    /// Spawns the task that releases this instance's Discord connection when
    /// another instance takes over the presence. While its connection stays
    /// open, Discord keeps showing its activity and masks the owner's.
    pub fn start_ownership_watch(&self) {
        let state = Arc::clone(&self.state);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(OWNERSHIP_CHECK_INTERVAL);
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut watch = OwnershipWatch::new(state.owner.is_owner());

            loop {
                interval.tick().await;
                if state.is_shutting_down() {
                    debug!("Ownership watch shutting down");
                    break;
                }

                if watch.lost_ownership(state.owner.is_owner())
                    && !Self::release_presence(&state).await
                {
                    watch.mark_owner();
                }
            }
        });
    }

    /// Releases this instance's presence. Returns false, without releasing, if
    /// ownership came back (or shutdown started) while waiting for the lock.
    async fn release_presence(state: &AppState) -> bool {
        let mut discord = state.discord.lock().await;

        // A user event may have reclaimed ownership while this task waited:
        // releasing now would drop the update this instance just sent.
        if state.owner.is_owner() || state.is_shutting_down() {
            return false;
        }

        match discord.release().await {
            Ok(()) => info!("Released Discord presence: another instance owns it"),
            Err(e) => warn!("Failed to release Discord presence: {}", e),
        }
        true
    }

    pub async fn shutdown(&self) -> Result<()> {
        if !self.state.mark_shutting_down() {
            debug!("Presence service shutdown already in progress");
            self.idle_manager.cancel_timeout().await;
            return Ok(());
        }

        self.idle_manager.cancel_timeout().await;

        let mut discord = self.state.discord.lock().await;
        let mut first_error = None;

        // Clearing from a background instance could wipe the presence the
        // owning instance just set, so only the owner clears on shutdown.
        if self.state.owner.is_owner() {
            if let Err(error) = discord.clear_activity().await {
                warn!(
                    "Failed to clear Discord activity during shutdown: {}",
                    error
                );
                first_error = Some(error);
            }
        } else {
            debug!("Another instance owns the presence, not clearing it on shutdown");
        }

        if let Err(error) = discord.kill().await {
            warn!("Failed to close Discord IPC during shutdown: {}", error);
            if first_error.is_none() {
                first_error = Some(error);
            }
        }

        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::owner::dead_test_pid;
    use crate::service::PresenceOwner;

    #[tokio::test]
    async fn test_shutdown_cancels_idle_timeout_and_marks_state() {
        let state = Arc::new(AppState::new());
        let service = PresenceService::new(Arc::clone(&state), PresenceConfig::default());

        service.shutdown().await.unwrap();

        assert!(state.is_shutting_down());
        assert!(!service.idle_manager.has_timeout().await);
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let state = Arc::new(AppState::new());
        let service = PresenceService::new(state, PresenceConfig::default());

        assert!(service.shutdown().await.is_ok());
        assert!(service.shutdown().await.is_ok());
    }

    #[tokio::test]
    async fn test_update_presence_is_ignored_during_shutdown() {
        let state = Arc::new(AppState::new());
        let service = PresenceService::new(Arc::clone(&state), PresenceConfig::default());

        assert!(state.mark_shutting_down());
        assert!(service.update_presence(None).await.is_ok());

        let last_document = state.last_document.lock().await;
        assert!(last_document.is_none());
    }

    #[tokio::test]
    async fn test_rapid_updates_batched_into_single_discord_call() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        let state = Arc::new(AppState::new());
        let config = PresenceConfig::default();
        let service = PresenceService::new(Arc::clone(&state), config);

        let workspace_root = PathBuf::from("C:/test");

        // Call update_presence 5 times rapidly (simulates rapid file switches)
        for i in 0..5 {
            let file_path = workspace_root.join(format!("file{}.rs", i));
            let url = Url::from_file_path(&file_path).unwrap();
            let doc = Document::new(&url, &workspace_root, None);
            assert!(service.update_presence(Some(doc)).await.is_ok());
            // Small delay between calls, but less than debounce window (500ms)
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }

        // Wait for debounce to settle (500ms + buffer)
        tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;

        // Verify the final document stored in state (should be last one)
        let last_doc = state.last_document.lock().await;
        assert!(last_doc.is_some());
        let last_filename = last_doc.as_ref().unwrap().get_filename().unwrap();
        assert_eq!(
            last_filename, "file4.rs",
            "Latest document should be stored"
        );
    }

    #[tokio::test]
    async fn test_latest_document_sent_after_debounce() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        let state = Arc::new(AppState::new());
        let config = PresenceConfig::default();
        let service = PresenceService::new(Arc::clone(&state), config);

        let workspace_root = PathBuf::from("C:/test");

        // Send doc1
        let file_path1 = workspace_root.join("file1.rs");
        let url1 = Url::from_file_path(&file_path1).unwrap();
        let doc1 = Document::new(&url1, &workspace_root, None);
        assert!(service.update_presence(Some(doc1.clone())).await.is_ok());

        // Quickly send doc2 (before debounce expires)
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        let file_path2 = workspace_root.join("file2.rs");
        let url2 = Url::from_file_path(&file_path2).unwrap();
        let doc2 = Document::new(&url2, &workspace_root, None);
        assert!(service.update_presence(Some(doc2.clone())).await.is_ok());

        // Wait for debounce to settle
        tokio::time::sleep(tokio::time::Duration::from_millis(700)).await;

        // Verify the last document stored should be doc2
        let last_document = state.last_document.lock().await;
        assert!(last_document.is_some());
        let last_filename = last_document.as_ref().unwrap().get_filename().unwrap();
        assert_eq!(
            last_filename, "file2.rs",
            "Latest document should be stored after debounce"
        );
    }

    /// In-memory log sink used to count events emitted by background tasks.
    #[derive(Clone, Default)]
    struct LogCapture(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl LogCapture {
        fn count(&self, needle: &str) -> usize {
            String::from_utf8_lossy(&self.0.lock().unwrap())
                .matches(needle)
                .count()
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_spaced_updates_fire_idle_action_only_once() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        // Every idle action ends with this warning in tests, because no Discord
        // client exists, so counting it counts how many idle timers fired.
        const IDLE_FIRED: &str = "from idle task";

        let capture = LogCapture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);

        let state = Arc::new(AppState::new());
        let service = PresenceService::new(Arc::clone(&state), PresenceConfig::default());
        let workspace_root = PathBuf::from("C:/test");

        // Three updates spaced beyond the debounce window, so each one reaches
        // perform_update_internal and re-arms the idle timer.
        for i in 0..3 {
            let url = Url::from_file_path(workspace_root.join(format!("file{i}.rs"))).unwrap();
            let doc = Document::new(&url, &workspace_root, None);
            assert!(service.update_presence(Some(doc)).await.is_ok());
            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        assert_eq!(
            capture.count(IDLE_FIRED),
            0,
            "No idle action before the timeout"
        );

        // Move past the default 300 s idle timeout, then let woken tasks run.
        tokio::time::advance(Duration::from_secs(301)).await;
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert_eq!(
            capture.count(IDLE_FIRED),
            1,
            "Only the timer armed by the latest update should fire"
        );
    }

    /// Logged once per release of this instance's presence.
    const RELEASED: &str = "Released Discord presence";
    /// PID assumed not to belong to any live process; used as "this instance".
    const THIS_PID: u32 = u32::MAX - 11;

    fn capture_logs() -> (LogCapture, tracing::subscriber::DefaultGuard) {
        let capture = LogCapture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        (capture, tracing::subscriber::set_default(subscriber))
    }

    /// App state whose ownership record currently names this instance.
    fn owned_state() -> Arc<AppState> {
        let owner = PresenceOwner::with_pid(THIS_PID);
        owner.set_record_owner(THIS_PID);
        Arc::new(AppState {
            owner: Arc::new(owner),
            ..AppState::new()
        })
    }

    /// Starts the watch and lets it record its first observation (as owner)
    /// before the test changes the ownership record.
    async fn start_watch(state: &Arc<AppState>) -> PresenceService {
        let service = PresenceService::new(Arc::clone(state), PresenceConfig::default());
        service.start_ownership_watch();
        tokio::time::sleep(OWNERSHIP_CHECK_INTERVAL * 2).await;
        service
    }

    #[test]
    fn test_ownership_watch_reports_only_the_transition() {
        let mut watch = OwnershipWatch::new(true);

        assert!(!watch.lost_ownership(true));
        assert!(watch.lost_ownership(false));
        assert!(!watch.lost_ownership(false));
        assert!(!watch.lost_ownership(true));
        assert!(watch.lost_ownership(false));
    }

    #[test]
    fn test_ownership_watch_detects_loss_again_after_reclaim_during_release() {
        let mut watch = OwnershipWatch::new(true);
        assert!(watch.lost_ownership(false));

        // The release found that ownership had already come back.
        watch.mark_owner();

        assert!(watch.lost_ownership(false));
    }

    #[tokio::test(start_paused = true)]
    async fn test_ownership_watch_releases_once_when_another_instance_takes_over() {
        let (capture, _guard) = capture_logs();
        let state = owned_state();
        let _service = start_watch(&state).await;

        // The test process itself plays the other, live instance.
        state.owner.set_record_owner(std::process::id());
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(
            capture.count(RELEASED),
            1,
            "Release must happen once on the transition, not on every tick"
        );
        state.owner.remove_record();
    }

    #[tokio::test(start_paused = true)]
    async fn test_ownership_watch_does_not_release_while_owner() {
        let (capture, _guard) = capture_logs();
        let state = owned_state();
        let _service = start_watch(&state).await;

        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(capture.count(RELEASED), 0);
        state.owner.remove_record();
    }

    #[tokio::test(start_paused = true)]
    async fn test_ownership_watch_does_not_release_when_record_is_missing() {
        let (capture, _guard) = capture_logs();
        let state = owned_state();
        let _service = start_watch(&state).await;

        state.owner.remove_record();
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(capture.count(RELEASED), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ownership_watch_does_not_release_when_owner_pid_is_dead() {
        let (capture, _guard) = capture_logs();
        let state = owned_state();
        let _service = start_watch(&state).await;

        state.owner.set_record_owner(dead_test_pid());
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(capture.count(RELEASED), 0);
        state.owner.remove_record();
    }

    #[tokio::test(start_paused = true)]
    async fn test_update_presence_does_not_wait_for_a_slow_discord_send() {
        use std::path::PathBuf;
        use tower_lsp::lsp_types::Url;

        let state = Arc::new(AppState::new());
        let service = PresenceService::new(Arc::clone(&state), PresenceConfig::default());
        let workspace_root = PathBuf::from("C:/test");
        let doc = |name: &str| {
            let url = Url::from_file_path(workspace_root.join(name)).unwrap();
            Document::new(&url, &workspace_root, None)
        };

        // Holding the Discord lock simulates a slow connect or send in progress.
        let discord_busy = state.discord.lock().await;

        assert!(service.update_presence(Some(doc("first.rs"))).await.is_ok());
        // Let the debounce expire so the update task blocks on the Discord lock.
        tokio::time::sleep(Duration::from_secs(1)).await;

        // A new editor event must not wait for that send to finish.
        let second_update = tokio::time::timeout(
            Duration::from_secs(1),
            service.update_presence(Some(doc("second.rs"))),
        )
        .await;
        assert!(
            second_update.is_ok(),
            "update_presence must not wait for the Discord lock"
        );

        drop(discord_busy);
    }

    #[tokio::test(start_paused = true)]
    async fn test_ownership_release_clears_once_and_keeps_the_connection() {
        // Logged at the start of every Discord::clear_activity call.
        const CLEARED: &str = "Clearing Discord activity";

        let (capture, _guard) = capture_logs();
        let state = owned_state();
        state.discord.lock().await.set_connected_for_test(true);
        let _service = start_watch(&state).await;

        // The test process itself plays the other, live instance.
        state.owner.set_record_owner(std::process::id());
        tokio::time::sleep(Duration::from_secs(5)).await;

        assert_eq!(capture.count(CLEARED), 1, "Clear once on the transition");
        assert!(
            state.discord.lock().await.is_connected(),
            "Release must keep the IPC connection open"
        );
        state.owner.remove_record();
    }
}
