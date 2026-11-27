// Per-browser conveniences only (nicknames, the open conversation). Storage
// can be missing or throw (private windows, blocked site data): never rely on it.
import { useCallback, useState } from "react";

export function load<T>(key: string, fallback: T): T {
  try {
    const raw = window.localStorage.getItem(key);
    return raw === null ? fallback : (JSON.parse(raw) as T);
  } catch {
    return fallback;
  }
}

export function save(key: string, value: unknown): void {
  try {
    window.localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Not saved: it is only a convenience.
  }
}

/** Local names for contacts. They never leave this browser. */
export function useNicknames() {
  const [names, setNames] = useState<Record<string, string>>(() => load("linkchat.nicknames", {}));

  const rename = useCallback((userId: string, name: string) => {
    setNames((current) => {
      const next = { ...current };
      const trimmed = name.trim();
      if (trimmed) next[userId] = trimmed;
      else delete next[userId];
      save("linkchat.nicknames", next);
      return next;
    });
  }, []);

  return [names, rename] as const;
}

/** How a contact is shown: its nickname, or the start of its UserId. */
export function displayName(userId: string, names: Record<string, string>): string {
  return names[userId] ?? `${userId.slice(0, 8)}…`;
}

/** Local photos for contacts (and yourself), as small data URLs. Never sent anywhere. */
export function usePhotos() {
  const [photos, setPhotos] = useState<Record<string, string>>(() => load("linkchat.photos", {}));

  const setPhoto = useCallback((userId: string, dataUrl: string | null) => {
    setPhotos((current) => {
      const next = { ...current };
      if (dataUrl) next[userId] = dataUrl;
      else delete next[userId];
      save("linkchat.photos", next);
      return next;
    });
  }, []);

  return [photos, setPhoto] as const;
}

const PHOTO_PX = 128;

/** A square, centre-cropped JPEG small enough for localStorage. */
export async function shrinkPhoto(file: File): Promise<string> {
  const bitmap = await createImageBitmap(file);
  const side = Math.min(bitmap.width, bitmap.height);
  const canvas = document.createElement("canvas");
  canvas.width = canvas.height = PHOTO_PX;
  const context = canvas.getContext("2d");
  if (!context) throw new Error("no 2d canvas");
  context.drawImage(
    bitmap,
    (bitmap.width - side) / 2,
    (bitmap.height - side) / 2,
    side,
    side,
    0,
    0,
    PHOTO_PX,
    PHOTO_PX,
  );
  bitmap.close();
  return canvas.toDataURL("image/jpeg", 0.85);
}
