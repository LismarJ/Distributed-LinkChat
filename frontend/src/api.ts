// The client API's contract.

export type MessageState = "pending" | "sent" | "not_delivered" | "received";

export interface Message {
  envelope_id: string;
  from_me: boolean;
  text: string;
  state: MessageState;
  sent_at: number;
  /** Shown although a message before it never arrived. */
  after_gap: boolean;
}

export interface Conversation {
  peer: string;
  /** Already in causal order: never reorder them here. */
  messages: Message[];
}

/** The WebSocket only says *that* a conversation changed. */
export interface ChangeEvent {
  kind: "message" | "state";
  peer: string;
  envelope_id: string;
}

export type RelayState = "ready" | "syncing" | "unreachable";

/** One relay as this client sees it right now (the overlay view, C10). */
export interface RelayStatus {
  name: string;
  state: RelayState;
  /** Records it holds: messages not yet acknowledged by their reader. */
  records: number;
  /** Acknowledged records it remembers, so a late copy is not stored again. */
  tombstones: number;
  /** Records it holds for a primary that was unreachable at write time. */
  hinted_records: number;
  /** One of the primaries of the open chat's mailbox. */
  holds_mailbox: boolean;
  /** Position in that mailbox's preference order. */
  rank: number;
}

export class ApiError extends Error {
  /** 0 when the request never got an answer. */
  readonly status: number;

  constructor(status: number, detail: string) {
    super(detail);
    this.status = status;
  }
}

async function call<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`/api${path}`, {
      ...init,
      headers: { "content-type": "application/json" },
    });
  } catch {
    throw new ApiError(0, "network error");
  }
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body: unknown = await response.json();
      if (body && typeof body === "object" && "detail" in body && typeof body.detail === "string") {
        detail = body.detail;
      }
    } catch {
      // Not JSON (a proxy error page, say): the status line will do.
    }
    throw new ApiError(response.status, detail);
  }
  return (await response.json()) as T;
}

const post = (body: unknown): RequestInit => ({ method: "POST", body: JSON.stringify(body) });

export const api = {
  me: () => call<{ user_id: string }>("/me").then((b) => b.user_id),
  contacts: () => call<{ contacts: string[] }>("/contacts").then((b) => b.contacts),
  addContact: (userId: string) => call<{ user_id: string }>("/contacts", post({ user_id: userId })),
  conversation: (peer: string) => call<Conversation>(`/conversations/${peer}`),
  relays: (peer: string | null) =>
    call<RelayStatus[]>(peer ? `/relays?peer=${encodeURIComponent(peer)}` : "/relays"),
  send: (peer: string, text: string) =>
    call<{ envelope_id: string; state: MessageState }>(
      `/conversations/${peer}/messages`,
      post({ text }),
    ),
};

/** A UserId: 64 lowercase hex digits. */
export const USER_ID = /^[0-9a-f]{64}$/;
