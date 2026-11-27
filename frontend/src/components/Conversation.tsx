import { type FormEvent, useEffect, useId, useLayoutEffect, useRef, useState } from "react";
import type { Conversation as ConversationData, Message, MessageState } from "../api";
import { displayName } from "../storage";
import { AvatarPicker } from "./Avatar";
import { Composer } from "./Composer";

const STATE: Record<MessageState, { icon: string; label: string; explain: string }> = {
  pending: {
    icon: "◷",
    label: "Pending",
    explain: "Not on the relays yet. It keeps retrying by itself.",
  },
  sent: {
    icon: "✓",
    label: "Sent",
    explain: "Stored on the relays. It reaches your contact when they come online.",
  },
  not_delivered: {
    icon: "!",
    label: "Not delivered",
    explain: "It expired before the relays could store it.",
  },
  received: { icon: "↓", label: "Received", explain: "Received from your contact." },
};

const TIME = new Intl.DateTimeFormat("en-GB", { hour: "2-digit", minute: "2-digit" });
const FULL = new Intl.DateTimeFormat("en-GB", { dateStyle: "medium", timeStyle: "short" });
const DATE = new Intl.DateTimeFormat("en-GB", { weekday: "long", day: "numeric", month: "long" });

function dayKey(when: Date): string {
  return `${when.getFullYear()}-${when.getMonth()}-${when.getDate()}`;
}

/** "Today", "Yesterday", or the date: a label between messages of different days. */
function dayLabel(when: Date): string {
  const today = new Date();
  const yesterday = new Date(today);
  yesterday.setDate(today.getDate() - 1);
  if (dayKey(when) === dayKey(today)) return "Today";
  if (dayKey(when) === dayKey(yesterday)) return "Yesterday";
  return DATE.format(when);
}

interface Props {
  peer: string;
  conversation: ConversationData | null;
  names: Record<string, string>;
  photos: Record<string, string>;
  onPhoto: (userId: string, dataUrl: string | null) => void;
  onRename: (peer: string, name: string) => void;
  onSent: () => void;
  onBack: () => void;
}

export function Conversation(props: Props) {
  const { peer, conversation, names, photos, onPhoto, onRename, onSent, onBack } = props;
  const messages = conversation?.peer === peer ? conversation.messages : null;
  const listRef = useRef<HTMLOListElement>(null);
  const stuckToBottom = useRef(true);

  // Follow new messages, unless the user scrolled up to read older ones.
  useLayoutEffect(() => {
    const list = listRef.current;
    if (list && stuckToBottom.current) list.scrollTop = list.scrollHeight;
  }, [messages]);

  useEffect(() => {
    stuckToBottom.current = true;
  }, [peer]);

  const onScroll = () => {
    const list = listRef.current;
    if (list) stuckToBottom.current = list.scrollHeight - list.scrollTop - list.clientHeight < 40;
  };

  return (
    <section className="conversation" aria-labelledby="conversation-title">
      <div className="conversation-head">
        <button type="button" className="back" onClick={onBack}>
          <span aria-hidden="true">←</span> Chats
        </button>
        <AvatarPicker
          userId={peer}
          name={names[peer]}
          photo={photos[peer]}
          whose={`${displayName(peer, names)}'s photo`}
          onPhoto={(p) => onPhoto(peer, p)}
          size="lg"
        />
        <Title peer={peer} names={names} onRename={onRename} />
      </div>
      <ol
        className="messages"
        ref={listRef}
        onScroll={onScroll}
        role="log"
        aria-labelledby="conversation-title"
      >
        {messages === null ? null : messages.length === 0 ? (
          <li className="empty-chat">
            <p>No messages yet.</p>
            <p className="muted small">
              Write the first one: it reaches them even if they are offline now.
            </p>
          </li>
        ) : (
          messages.map((m, i) => {
            const when = new Date(m.sent_at * 1000);
            const before = i > 0 ? new Date(messages[i - 1].sent_at * 1000) : null;
            const newDay = !before || dayKey(before) !== dayKey(when);
            return (
              <MessageItem
                key={m.envelope_id}
                message={m}
                day={newDay ? dayLabel(when) : null}
              />
            );
          })
        )}
      </ol>
      <Composer key={peer} peer={peer} onSent={onSent} />
    </section>
  );
}

function MessageItem({ message: m, day }: { message: Message; day: string | null }) {
  const state = STATE[m.state];
  const when = new Date(m.sent_at * 1000);
  return (
    <li className={m.from_me ? "message mine" : "message theirs"}>
      {day && <p className="day">{day}</p>}
      {m.after_gap && (
        <p className="gap">An earlier message may be missing: it has not arrived yet.</p>
      )}
      <div className={`bubble state-${m.state}`}>
        <p className="text">{m.text}</p>
        <p className="meta">
          <time dateTime={when.toISOString()} title={FULL.format(when)}>
            {TIME.format(when)}
          </time>
          {m.from_me && (
            <span className="state" title={state.explain}>
              <span aria-hidden="true">{state.icon}</span> {state.label}
            </span>
          )}
        </p>
      </div>
    </li>
  );
}

function Title({ peer, names, onRename }: Pick<Props, "peer" | "names" | "onRename">) {
  const id = useId();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");

  const start = () => {
    setDraft(names[peer] ?? "");
    setEditing(true);
  };
  const submit = (event: FormEvent) => {
    event.preventDefault();
    onRename(peer, draft);
    setEditing(false);
  };

  return (
    <div className="title">
      {editing ? (
        <form className="rename" onSubmit={submit}>
          <label htmlFor={id}>Nickname</label>
          <input
            id={id}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            autoComplete="off"
            autoFocus
          />
          <button type="submit">Save</button>
          <button type="button" onClick={() => setEditing(false)}>
            Cancel
          </button>
        </form>
      ) : (
        <>
          <h2 id="conversation-title">{displayName(peer, names)}</h2>
          <button type="button" className="link-button" onClick={start}>
            Rename
          </button>
        </>
      )}
      <code className="user-id small">{peer}</code>
    </div>
  );
}
