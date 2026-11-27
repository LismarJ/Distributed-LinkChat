// The relay tier as this client sees it (C10). Polled, because a relay dying is
// not a conversation event and never arrives on the WebSocket. Renders whatever
// the API returns: no relay count or copy count is assumed here.
import { useEffect, useId, useState } from "react";
import { type RelayState, type RelayStatus, api } from "../api";

const POLL_MS = 2000;

const STATE: Record<RelayState, { icon: string; label: string }> = {
  ready: { icon: "●", label: "Ready" },
  syncing: { icon: "◐", label: "Syncing" },
  unreachable: { icon: "✕", label: "Unreachable" },
};

interface Props {
  peer: string | null;
  peerName: string | null;
  onClose: () => void;
}

export function RelayPanel({ peer, peerName, onClose }: Props) {
  const titleId = useId();
  const [relays, setRelays] = useState<RelayStatus[] | null>(null);
  const [stale, setStale] = useState(false);

  useEffect(() => {
    let cancelled = false;
    let busy = false;

    const tick = async () => {
      if (busy || document.hidden) return;
      busy = true;
      try {
        const fresh = await api.relays(peer);
        if (!cancelled) {
          setRelays(fresh);
          setStale(false);
        }
      } catch {
        // This client may itself be restarting: keep the last view, say so.
        if (!cancelled) setStale(true);
      } finally {
        busy = false;
      }
    };

    setRelays(null);
    void tick();
    const timer = window.setInterval(() => void tick(), POLL_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [peer]);

  const down = relays?.filter((r) => r.state !== "ready").length ?? 0;

  return (
    <aside className="relays" aria-labelledby={titleId}>
      <div className="relays-head">
        <h2 id={titleId}>Relays</h2>
        <button type="button" className="icon-button" onClick={onClose} aria-label="Hide relays">
          ✕
        </button>
      </div>
      <p className="muted small">
        {peer
          ? `In preference order for your chat with ${peerName}. The primaries must each hold every message; when one is down, the next relay stands in.`
          : "Open a chat to see which relays hold its messages."}
      </p>
      {relays && (
        <p className="relays-summary" role="status">
          {down === 0
            ? `All ${relays.length} relays are up.`
            : `${down} of ${relays.length} relays ${down === 1 ? "is" : "are"} not ready.`}
        </p>
      )}
      {stale && (
        <p className="error" role="alert">
          Cannot reach your client: this view may be out of date.
        </p>
      )}
      {relays === null ? (
        <p className="muted small">Asking the relays…</p>
      ) : (
        <ol className="relay-list">
          {relays.map((relay) => (
            <RelayCard key={relay.name} relay={relay} />
          ))}
        </ol>
      )}
    </aside>
  );
}

function RelayCard({ relay }: { relay: RelayStatus }) {
  const state = STATE[relay.state] ?? STATE.unreachable;
  return (
    <li className={`relay relay-${relay.state}`}>
      <div className="relay-top">
        <span className="relay-name">{relay.name}</span>
        {relay.holds_mailbox && <span className="badge">Primary</span>}
        <span className="relay-state">
          <span aria-hidden="true">{state.icon}</span> {state.label}
        </span>
      </div>
      <dl className="relay-stats">
        <div title="Messages on this relay that their reader has not acknowledged yet">
          <dt>Held</dt>
          <dd>{relay.records}</dd>
        </div>
        <div title="Acknowledged messages it remembers, so a late copy is not stored again">
          <dt>Acked</dt>
          <dd>{relay.tombstones}</dd>
        </div>
        <div title="Messages it keeps for a primary that was down when they were written">
          <dt>Standing in</dt>
          <dd>{relay.hinted_records}</dd>
        </div>
      </dl>
    </li>
  );
}
