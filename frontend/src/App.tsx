// Holds what the API says and refetches on every hint from the WebSocket.
// Nothing here is a source of truth: the client's history is.
import { useCallback, useEffect, useRef, useState } from "react";
import { type ChangeEvent, type Conversation as ConversationData, api } from "./api";
import { Contacts } from "./components/Contacts";
import { Conversation } from "./components/Conversation";
import { Header } from "./components/Header";
import { RelayPanel } from "./components/RelayPanel";
import { Welcome } from "./components/Welcome";
import { displayName, load, save, useNicknames, usePhotos } from "./storage";
import { useLiveEvents } from "./useLiveEvents";

export function App() {
  const [me, setMe] = useState<string | null>(null);
  const [contacts, setContacts] = useState<string[]>([]);
  const [selected, setSelected] = useState<string | null>(() => load("linkchat.selected", null));
  const [conversation, setConversation] = useState<ConversationData | null>(null);
  const [unread, setUnread] = useState<ReadonlySet<string>>(new Set());
  const [names, rename] = useNicknames();
  const [photos, setPhoto] = usePhotos();
  // Open by default where there is room for a third column.
  const [relaysOpen, setRelaysOpen] = useState<boolean>(() =>
    load("linkchat.relays", window.matchMedia("(min-width: 1100px)").matches),
  );
  const toggleRelays = useCallback(() => {
    setRelaysOpen((open) => {
      save("linkchat.relays", !open);
      return !open;
    });
  }, []);

  const selectedRef = useRef(selected);
  const contactsRef = useRef(contacts);
  useEffect(() => {
    contactsRef.current = contacts;
  }, [contacts]);
  const request = useRef(0);

  const loadConversation = useCallback(async (peer: string) => {
    const mine = ++request.current;
    try {
      const fresh = await api.conversation(peer);
      // A slower, older answer must not overwrite a newer one.
      if (mine === request.current && selectedRef.current === peer) setConversation(fresh);
    } catch {
      // Stale until the next event or reconnect: never wrong.
    }
  }, []);

  const loadContacts = useCallback(async () => {
    try {
      setContacts(await api.contacts());
    } catch {
      // Same: the next reconnect refetches.
    }
  }, []);

  const loadAll = useCallback(async () => {
    try {
      setMe(await api.me());
    } catch {
      // Shown through the link status; retried on reconnect.
    }
    await loadContacts();
    if (selectedRef.current) await loadConversation(selectedRef.current);
  }, [loadContacts, loadConversation]);

  const onEvent = useCallback(
    (event: ChangeEvent) => {
      if (event.peer === selectedRef.current) {
        void loadConversation(event.peer);
      } else if (event.kind === "message") {
        setUnread((current) => new Set(current).add(event.peer));
      }
      if (!contactsRef.current.includes(event.peer)) void loadContacts();
    },
    [loadContacts, loadConversation],
  );

  const link = useLiveEvents(onEvent, () => void loadAll());

  useEffect(() => {
    void loadAll();
  }, [loadAll]);

  const select = useCallback(
    (peer: string | null) => {
      selectedRef.current = peer;
      setSelected(peer);
      save("linkchat.selected", peer);
      setConversation(null);
      if (peer) {
        setUnread((current) => {
          const next = new Set(current);
          next.delete(peer);
          return next;
        });
        void loadConversation(peer);
      }
    },
    [loadConversation],
  );

  const added = useCallback(
    async (peer: string, nickname: string) => {
      if (nickname.trim()) rename(peer, nickname);
      await loadContacts();
      select(peer);
    },
    [loadContacts, rename, select],
  );

  // Forget a remembered conversation whose contact this client no longer has.
  const shown = selected && contacts.includes(selected) ? selected : null;

  return (
    <div
      className="app"
      data-view={shown ? "conversation" : "contacts"}
      data-relays={relaysOpen ? "open" : "closed"}
    >
      <Header link={link} relaysOpen={relaysOpen} onToggleRelays={toggleRelays} />
      <main className="body">
        <Contacts
          me={me}
          contacts={contacts}
          selected={shown}
          unread={unread}
          names={names}
          photos={photos}
          onPhoto={setPhoto}
          onSelect={select}
          onAdded={(peer, nickname) => void added(peer, nickname)}
        />
        {shown ? (
          <Conversation
            peer={shown}
            conversation={conversation}
            names={names}
            photos={photos}
            onPhoto={setPhoto}
            onRename={rename}
            onSent={() => void loadConversation(shown)}
            onBack={() => select(null)}
          />
        ) : (
          <Welcome hasContacts={contacts.length > 0} />
        )}
        {relaysOpen && (
          <RelayPanel
            peer={shown}
            peerName={shown ? displayName(shown, names) : null}
            onClose={toggleRelays}
          />
        )}
      </main>
    </div>
  );
}
