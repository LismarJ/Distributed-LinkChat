import { Logo } from "./Header";

/** What to do when no conversation is open: how a conversation starts. */
export function Welcome({ hasContacts }: { hasContacts: boolean }) {
  return (
    <section className="conversation welcome" aria-labelledby="welcome-title">
      <div className="welcome-card">
        <Logo />
        <h2 id="welcome-title">
          {hasContacts ? "Pick a chat to start" : "Welcome to LinkChat"}
        </h2>
        <p className="muted">
          Nobody finds you here by chance: there is no user directory. Only people who
          have your ID can write to you, and you can only write to people who gave you
          theirs.
        </p>
        <ol className="steps">
          <li>
            <span>
              <strong>Copy your ID</strong> with “Copy my ID”.
            </span>
          </li>
          <li>
            <span>
              <strong>Send it</strong> to your contact over a channel you already share.
            </span>
          </li>
          <li>
            <span>
              <strong>Paste theirs</strong> into “New chat” and say hello.
            </span>
          </li>
        </ol>
        <p className="muted small">
          Messages are end-to-end encrypted. If your contact is offline, the relays keep
          them until they come back.
        </p>
      </div>
    </section>
  );
}
