//! Live-task census for the mesh's per-connection task families.
//!
//! A reconnect storm plus peer churn can leave thousands of short-lived tokio
//! tasks alive at once, each holding a connection and its buffers; the host's
//! RSS then stays at that peak because the allocator keeps the pages. This
//! module counts, per task family, how many tasks are alive now and the most
//! that were alive since the last report, so an operator can see which family
//! peaks. A [`TaskGuard`] is created before the task is spawned and moved into
//! it, so a task that is cancelled before its first poll is still counted
//! and uncounted exactly once.
//!
//! The counters belong to one [`crate::mesh::Node`]: a process can serve more
//! than one mesh, and one mesh's tasks must not hide another's.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// How often [`spawn_reporter`] logs the census.
const REPORT_INTERVAL: Duration = Duration::from_secs(30);

/// A family of tasks the mesh spawns per connection or per inbound event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TaskKind {
    /// `handle_incoming` from accept until the connection is registered.
    AcceptHandshake,
    /// `handle_control_incoming` for the whole control connection.
    ControlAccept,
    /// The stream dispatcher of one live connection, inbound or outbound.
    Dispatcher,
    /// A dispatcher's recovery after its connection closed, including the
    /// redial backoff wait, the redial and its gossip.
    DispatcherRecovery,
    /// The sleep that paces a redial after a close (inside recovery).
    RedialBackoffWait,
    GossipStream,
    RouteRequestStream,
    TunnelMapStream,
    PluginStream,
    SubprotocolStream,
    PeerDownStream,
    PeerLeavingStream,
    /// An outbound dial to a peer found in someone else's gossip.
    DiscoveryConnect,
    /// The background task that runs the discovery dials of one gossip.
    DiscoverySpawn,
    /// Waits out the grace period of a replaced connection.
    ReplacedDrain,
    ReconnectGossip,
    PathRecheck,
}

impl TaskKind {
    const ALL: [TaskKind; 17] = [
        TaskKind::AcceptHandshake,
        TaskKind::ControlAccept,
        TaskKind::Dispatcher,
        TaskKind::DispatcherRecovery,
        TaskKind::RedialBackoffWait,
        TaskKind::GossipStream,
        TaskKind::RouteRequestStream,
        TaskKind::TunnelMapStream,
        TaskKind::PluginStream,
        TaskKind::SubprotocolStream,
        TaskKind::PeerDownStream,
        TaskKind::PeerLeavingStream,
        TaskKind::DiscoveryConnect,
        TaskKind::DiscoverySpawn,
        TaskKind::ReplacedDrain,
        TaskKind::ReconnectGossip,
        TaskKind::PathRecheck,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            TaskKind::AcceptHandshake => "accept_handshake",
            TaskKind::ControlAccept => "control_accept",
            TaskKind::Dispatcher => "dispatcher",
            TaskKind::DispatcherRecovery => "dispatcher_recovery",
            TaskKind::RedialBackoffWait => "redial_backoff_wait",
            TaskKind::GossipStream => "gossip_stream",
            TaskKind::RouteRequestStream => "route_request_stream",
            TaskKind::TunnelMapStream => "tunnel_map_stream",
            TaskKind::PluginStream => "plugin_stream",
            TaskKind::SubprotocolStream => "subprotocol_stream",
            TaskKind::PeerDownStream => "peer_down_stream",
            TaskKind::PeerLeavingStream => "peer_leaving_stream",
            TaskKind::DiscoveryConnect => "discovery_connect",
            TaskKind::DiscoverySpawn => "discovery_spawn",
            TaskKind::ReplacedDrain => "replaced_drain",
            TaskKind::ReconnectGossip => "reconnect_gossip",
            TaskKind::PathRecheck => "path_recheck",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Default)]
struct Slot {
    live: AtomicI64,
    /// Highest `live` since the last report took it.
    peak: AtomicI64,
    /// Tasks started since the process began; a report whose total moved
    /// shows churn even when every task finished between two reports.
    started: AtomicU64,
}

/// One family's numbers at one moment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct KindCount {
    pub(crate) kind: TaskKind,
    pub(crate) live: i64,
    pub(crate) peak: i64,
    pub(crate) started: u64,
}

/// Per-node live-task counters, one slot per [`TaskKind`].
pub(crate) struct TaskCensus {
    slots: [Slot; TaskKind::ALL.len()],
}

impl Default for TaskCensus {
    fn default() -> Self {
        Self {
            slots: std::array::from_fn(|_| Slot::default()),
        }
    }
}

impl TaskCensus {
    /// Count one task of `kind` until the returned guard is dropped.
    pub(crate) fn enter(self: &Arc<Self>, kind: TaskKind) -> TaskGuard {
        let slot = &self.slots[kind.index()];
        slot.started.fetch_add(1, Ordering::Relaxed);
        let live = slot.live.fetch_add(1, Ordering::Relaxed) + 1;
        slot.peak.fetch_max(live, Ordering::Relaxed);
        TaskGuard {
            census: Arc::clone(self),
            kind,
        }
    }

    /// Every family's numbers. With `take_peak` the peaks restart from the
    /// current live counts, so each report covers its own interval.
    pub(crate) fn snapshot(&self, take_peak: bool) -> Vec<KindCount> {
        TaskKind::ALL
            .iter()
            .map(|&kind| {
                let slot = &self.slots[kind.index()];
                let live = slot.live.load(Ordering::Relaxed);
                let peak = if take_peak {
                    slot.peak.swap(live, Ordering::Relaxed).max(live)
                } else {
                    slot.peak.load(Ordering::Relaxed)
                };
                KindCount {
                    kind,
                    live,
                    peak,
                    started: slot.started.load(Ordering::Relaxed),
                }
            })
            .collect()
    }
}

/// Uncounts its task when dropped, including when the task is cancelled.
#[must_use = "the task is only counted while its guard is alive"]
pub(crate) struct TaskGuard {
    census: Arc<TaskCensus>,
    kind: TaskKind,
}

impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.census.slots[self.kind.index()]
            .live
            .fetch_sub(1, Ordering::Relaxed);
    }
}

/// Log `live_tasks` every [`REPORT_INTERVAL`] while any family had a task
/// alive or started since the previous report, and once more when they have
/// all finished, so a peak that ended between two reports is still seen.
pub(crate) fn spawn_reporter(census: Arc<TaskCensus>) {
    tokio::spawn(async move {
        let mut last_started = 0u64;
        let mut last_active = false;
        loop {
            tokio::time::sleep(REPORT_INTERVAL).await;
            let counts = census.snapshot(true);
            let started: u64 = counts.iter().map(|c| c.started).sum();
            let active = counts.iter().any(|c| c.live > 0 || c.peak > 0);
            if started == last_started && !last_active && !active {
                continue;
            }
            last_started = started;
            last_active = active;
            tracing::info!(live_tasks = %format_counts(&counts), "live_tasks");
        }
    });
}

fn format_counts(counts: &[KindCount]) -> String {
    counts
        .iter()
        .filter(|c| c.live > 0 || c.peak > 0 || c.started > 0)
        .map(|c| {
            format!(
                "{}=live:{}/peak:{}/started:{}",
                c.kind.label(),
                c.live,
                c.peak,
                c.started
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count(census: &TaskCensus, kind: TaskKind) -> KindCount {
        census
            .snapshot(false)
            .into_iter()
            .find(|c| c.kind == kind)
            .unwrap()
    }

    #[test]
    fn guard_counts_until_dropped_and_tracks_peak() {
        let census = Arc::new(TaskCensus::default());
        let a = census.enter(TaskKind::Dispatcher);
        let b = census.enter(TaskKind::Dispatcher);
        assert_eq!(count(&census, TaskKind::Dispatcher).live, 2);
        drop(a);
        drop(b);
        let c = count(&census, TaskKind::Dispatcher);
        assert_eq!((c.live, c.peak, c.started), (0, 2, 2));
    }

    #[test]
    fn snapshot_that_takes_the_peak_restarts_it_from_live() {
        let census = Arc::new(TaskCensus::default());
        let a = census.enter(TaskKind::GossipStream);
        let b = census.enter(TaskKind::GossipStream);
        drop(b);
        let first = census.snapshot(true);
        assert_eq!(
            first
                .iter()
                .find(|c| c.kind == TaskKind::GossipStream)
                .unwrap()
                .peak,
            2
        );
        assert_eq!(count(&census, TaskKind::GossipStream).peak, 1);
        drop(a);
    }

    #[tokio::test]
    async fn task_aborted_before_its_first_poll_is_uncounted() {
        let census = Arc::new(TaskCensus::default());
        let guard = census.enter(TaskKind::ReplacedDrain);
        let handle = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        assert_eq!(count(&census, TaskKind::ReplacedDrain).live, 1);
        handle.abort();
        let _ = handle.await;
        let c = count(&census, TaskKind::ReplacedDrain);
        assert_eq!((c.live, c.started), (0, 1));
    }

    #[test]
    fn kinds_have_distinct_slots_and_labels() {
        let mut labels: Vec<_> = TaskKind::ALL.iter().map(|k| k.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), TaskKind::ALL.len());
        for (i, kind) in TaskKind::ALL.iter().enumerate() {
            assert_eq!(kind.index(), i);
        }
    }

    #[test]
    fn report_names_only_families_that_ran() {
        let census = Arc::new(TaskCensus::default());
        let _g = census.enter(TaskKind::PathRecheck);
        let text = format_counts(&census.snapshot(false));
        assert_eq!(text, "path_recheck=live:1/peak:1/started:1");
    }
}
