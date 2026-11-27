// The WebSocket at /api/ws, reconnected with backoff for as long as the page lives.
import { useEffect, useRef, useState } from "react";
import type { ChangeEvent } from "./api";

export type Link = "connecting" | "open" | "closed";

const FIRST_RETRY_MS = 500;
const LAST_RETRY_MS = 10_000;

/**
 * Calls `onEvent` for every change, and `onOpen` every time the socket
 * (re)opens: events sent while it was down are lost, so refetch everything then.
 */
export function useLiveEvents(onEvent: (e: ChangeEvent) => void, onOpen: () => void): Link {
  const [link, setLink] = useState<Link>("connecting");
  const handlers = useRef({ onEvent, onOpen });

  useEffect(() => {
    handlers.current = { onEvent, onOpen };
  });

  useEffect(() => {
    let socket: WebSocket | undefined;
    let timer: number | undefined;
    let delay = FIRST_RETRY_MS;
    let stopped = false;

    const connect = () => {
      setLink("connecting");
      const url = new URL("/api/ws", window.location.href);
      url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
      const ws = new WebSocket(url);
      socket = ws;
      ws.onopen = () => {
        delay = FIRST_RETRY_MS;
        setLink("open");
        handlers.current.onOpen();
      };
      ws.onmessage = (message: MessageEvent<string>) => {
        try {
          handlers.current.onEvent(JSON.parse(message.data) as ChangeEvent);
        } catch {
          // A malformed frame changes nothing: the next one, or a refetch, will.
        }
      };
      ws.onclose = () => {
        if (stopped) return;
        setLink("closed");
        timer = window.setTimeout(connect, delay * (0.75 + Math.random() / 2));
        delay = Math.min(delay * 2, LAST_RETRY_MS);
      };
    };

    connect();
    return () => {
      stopped = true;
      window.clearTimeout(timer);
      socket?.close();
    };
  }, []);

  return link;
}
