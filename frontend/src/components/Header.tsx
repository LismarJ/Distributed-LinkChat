import type { Link } from "../useLiveEvents";

const LINK_TEXT: Record<Link, string> = {
  open: "Connected",
  connecting: "Connecting…",
  closed: "Cannot reach your client. Retrying…",
};

export function Logo() {
  return (
    <svg className="logo" viewBox="0 0 32 32" aria-hidden="true">
      <defs>
        <linearGradient id="logo-g" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0" stopColor="#6366f1" />
          <stop offset="1" stopColor="#a855f7" />
        </linearGradient>
      </defs>
      <path
        d="M8 3h16a6 6 0 0 1 6 6v9a6 6 0 0 1-6 6h-9l-6.5 5v-5H8a6 6 0 0 1-6-6V9a6 6 0 0 1 6-6z"
        fill="url(#logo-g)"
      />
      <rect x="11.5" y="12.5" width="9" height="7" rx="1.6" fill="#fff" />
      <path
        d="M13.5 12.5v-2a2.5 2.5 0 0 1 5 0v2"
        stroke="#fff"
        strokeWidth="1.8"
        fill="none"
      />
    </svg>
  );
}

interface Props {
  link: Link;
  relaysOpen: boolean;
  onToggleRelays: () => void;
}

/** The brand, whether the browser can reach this user's client, and the relay view. */
export function Header({ link, relaysOpen, onToggleRelays }: Props) {
  return (
    <header className="header">
      <div className="brand">
        <Logo />
        <h1>LinkChat</h1>
      </div>
      <div className="header-end">
        <p className={`link link-${link}`} role="status">
          <span className="dot" aria-hidden="true" />
          {LINK_TEXT[link]}
        </p>
        <button
          type="button"
          className="toggle"
          aria-pressed={relaysOpen}
          onClick={onToggleRelays}
        >
          Relays
        </button>
      </div>
    </header>
  );
}
