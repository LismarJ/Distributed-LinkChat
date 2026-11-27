import { type FormEvent, type KeyboardEvent, useId, useRef, useState } from "react";
import { ApiError, api } from "../api";

function explain(error: unknown): string {
  if (!(error instanceof ApiError)) return "Could not send.";
  switch (error.status) {
    case 409:
      return "Your contact has never been online, so the relays do not have their keys yet. You can write to them once they have connected once.";
    case 503:
      return "No relay is answering right now, so your contact's keys could not be fetched. Try again in a moment.";
    case 413:
      return "This message is too long. The limit is in bytes: emoji and accented letters take more than one.";
    default:
      return "Your client is not answering. The message was not sent; try again.";
  }
}

/** Keeps the draft on any failure: nothing typed is ever lost. */
export function Composer({ peer, onSent }: { peer: string; onSent: () => void }) {
  const id = useId();
  const [text, setText] = useState("");
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const box = useRef<HTMLTextAreaElement>(null);

  const send = async () => {
    if (!text.trim() || busy) return;
    setBusy(true);
    try {
      await api.send(peer, text);
      setText("");
      setError("");
      onSent();
    } catch (e) {
      setError(explain(e));
    } finally {
      setBusy(false);
      box.current?.focus();
    }
  };

  const onSubmit = (event: FormEvent) => {
    event.preventDefault();
    void send();
  };

  // Enter sends, Shift+Enter is a new line; never while an IME is composing.
  const onKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>) => {
    if (event.key === "Enter" && !event.shiftKey && !event.nativeEvent.isComposing) {
      event.preventDefault();
      void send();
    }
  };

  return (
    <form className="composer" onSubmit={onSubmit}>
      <label htmlFor={id} className="visually-hidden">
        Message
      </label>
      <textarea
        id={id}
        ref={box}
        rows={2}
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={onKeyDown}
        aria-describedby={`${id}-error`}
        aria-invalid={error ? "true" : undefined}
        placeholder="Write a message"
        enterKeyHint="send"
      />
      <button type="submit" className="send" disabled={busy || !text.trim()} aria-label="Send">
        <svg viewBox="0 0 24 24" aria-hidden="true">
          <path d="M3.4 20.4 21 12 3.4 3.6 3.4 10l12.6 2-12.6 2z" fill="currentColor" />
        </svg>
      </button>
      <p className="error" id={`${id}-error`} aria-live="polite">
        {error}
      </p>
    </form>
  );
}
