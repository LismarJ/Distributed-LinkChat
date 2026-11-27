import { type FormEvent, useId, useRef, useState } from "react";
import { ApiError, USER_ID, api } from "../api";
import { displayName } from "../storage";
import { Avatar, AvatarPicker } from "./Avatar";

interface Props {
  me: string | null;
  contacts: string[];
  selected: string | null;
  unread: ReadonlySet<string>;
  names: Record<string, string>;
  photos: Record<string, string>;
  onPhoto: (userId: string, dataUrl: string | null) => void;
  onSelect: (peer: string) => void;
  onAdded: (peer: string, nickname: string) => void;
}

export function Contacts(props: Props) {
  const { me, contacts, selected, unread, names, photos, onSelect } = props;
  return (
    <aside className="sidebar">
      <Profile me={me} photo={me ? photos[me] : undefined} onPhoto={props.onPhoto} />
      <nav aria-label="Chats" className="chats">
        <h2 className="section-title">Chats</h2>
        {contacts.length === 0 ? (
          <p className="empty">No chats yet.</p>
        ) : (
          <ul>
            {contacts.map((peer) => (
              <li key={peer}>
                <button
                  type="button"
                  className="contact"
                  aria-current={peer === selected ? "true" : undefined}
                  onClick={() => onSelect(peer)}
                >
                  <Avatar userId={peer} name={names[peer]} photo={photos[peer]} />
                  <span className="contact-text">
                    <span className="contact-name">{displayName(peer, names)}</span>
                    <span className="contact-id">{peer.slice(0, 16)}…</span>
                  </span>
                  {unread.has(peer) && (
                    <span className="unread">
                      <span className="visually-hidden">new messages</span>
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
        )}
      </nav>
      <NewChat me={me} startOpen={contacts.length === 0} onAdded={props.onAdded} />
    </aside>
  );
}

/** You: your photo (local) and your ID, which is your invitation. */
function Profile({
  me,
  photo,
  onPhoto,
}: {
  me: string | null;
  photo?: string;
  onPhoto: Props["onPhoto"];
}) {
  const [note, setNote] = useState("");
  const idRef = useRef<HTMLElement>(null);

  const copy = async () => {
    if (!me) return;
    try {
      await navigator.clipboard.writeText(me);
      setNote("Copied! Send it to whoever wants to write to you.");
    } catch {
      // No clipboard outside a secure context: select it for Ctrl+C instead.
      const range = document.createRange();
      if (idRef.current) range.selectNodeContents(idRef.current);
      window.getSelection()?.removeAllRanges();
      window.getSelection()?.addRange(range);
      setNote("Selected: copy it with Ctrl+C.");
    }
    window.setTimeout(() => setNote(""), 3500);
  };

  return (
    <section className="profile" aria-labelledby="profile-title">
      <div className="profile-row">
        {me ? (
          <AvatarPicker userId={me} name="You" photo={photo} whose="your photo" onPhoto={(p) => onPhoto(me, p)} />
        ) : (
          <span className="avatar avatar-md avatar-loading" aria-hidden="true" />
        )}
        <div className="profile-text">
          <h2 id="profile-title">You</h2>
          <p className="muted small">Your ID is your invitation</p>
        </div>
      </div>
      <code className="user-id" ref={idRef}>
        {me ?? "…"}
      </code>
      <button type="button" className="primary" onClick={copy} disabled={!me}>
        Copy my ID
      </button>
      <p className="note" role="status">
        {note}
      </p>
    </section>
  );
}

function NewChat({
  me,
  startOpen,
  onAdded,
}: {
  me: string | null;
  startOpen: boolean;
  onAdded: Props["onAdded"];
}) {
  const id = useId();
  const details = useRef<HTMLDetailsElement>(null);
  const [userId, setUserId] = useState("");
  const [nickname, setNickname] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    const candidate = userId.trim().toLowerCase();
    if (!USER_ID.test(candidate)) {
      setError("An ID is 64 characters: digits 0 to 9 and letters a to f.");
      return;
    }
    if (candidate === me) {
      setError("That is your own ID.");
      return;
    }
    setBusy(true);
    try {
      await api.addContact(candidate);
      onAdded(candidate, nickname);
      setUserId("");
      setNickname("");
      setError("");
      if (details.current) details.current.open = false;
    } catch (e) {
      setError(
        e instanceof ApiError && e.status === 422
          ? "That ID is not valid."
          : "Could not add it: your client is not answering.",
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <details className="new-chat" ref={details} open={startOpen || undefined}>
      <summary>
        <span aria-hidden="true">＋</span> New chat
      </summary>
      <form onSubmit={submit} noValidate>
        <label htmlFor={`${id}-uid`}>Your contact's ID</label>
        <p className="hint" id={`${id}-hint`}>
          Ask them for it another way (in person, by email…): they see it in their LinkChat, under “You”.
        </p>
        <input
          id={`${id}-uid`}
          className="mono"
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          aria-describedby={`${id}-hint ${id}-error`}
          aria-invalid={error ? "true" : undefined}
          autoComplete="off"
          autoCapitalize="off"
          spellCheck={false}
          required
        />
        <label htmlFor={`${id}-nick`}>Name (optional, only for you)</label>
        <input
          id={`${id}-nick`}
          value={nickname}
          onChange={(e) => setNickname(e.target.value)}
          autoComplete="off"
        />
        <p className="error" id={`${id}-error`} aria-live="polite">
          {error}
        </p>
        <button type="submit" className="primary" disabled={busy}>
          Start chat
        </button>
      </form>
    </details>
  );
}
