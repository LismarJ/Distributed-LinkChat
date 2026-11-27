import { type ChangeEvent, useId, useState } from "react";
import { shrinkPhoto } from "../storage";

/** Two hues from the UserId: the same person always gets the same colours. */
function hues(userId: string): [number, number] {
  const a = parseInt(userId.slice(0, 4), 16) || 0;
  const b = parseInt(userId.slice(4, 8), 16) || 0;
  const first = a % 360;
  return [first, (first + 30 + (b % 60)) % 360];
}

function initials(userId: string, name?: string): string {
  if (name) {
    const words = name.trim().split(/\s+/).filter(Boolean);
    const letters = words.slice(0, 2).map((w) => [...w][0] ?? "");
    if (letters.join("")) return letters.join("").toUpperCase();
  }
  return userId.slice(0, 2).toUpperCase();
}

interface AvatarProps {
  userId: string;
  name?: string;
  photo?: string;
  size?: "sm" | "md" | "lg";
}

/** Decorative: the name is always written next to it. */
export function Avatar({ userId, name, photo, size = "md" }: AvatarProps) {
  const [h1, h2] = hues(userId);
  return (
    <span
      className={`avatar avatar-${size}`}
      aria-hidden="true"
      style={{
        backgroundImage: photo
          ? undefined
          : `linear-gradient(135deg, hsl(${h1} 62% 40%), hsl(${h2} 68% 32%))`,
      }}
    >
      {photo ? <img src={photo} alt="" /> : initials(userId, name)}
    </span>
  );
}

interface PickerProps extends AvatarProps {
  /** What the photo is of, for the accessible label: "your photo", "Bob's photo". */
  whose: string;
  onPhoto: (dataUrl: string | null) => void;
}

/** The avatar as a control: pick a local photo, or remove it. */
export function AvatarPicker({ whose, onPhoto, ...avatar }: PickerProps) {
  const id = useId();
  const [error, setError] = useState("");

  const pick = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;
    try {
      onPhoto(await shrinkPhoto(file));
      setError("");
    } catch {
      setError("That image could not be read.");
    }
  };

  return (
    <div className="avatar-picker">
      <label htmlFor={id} className="avatar-label" title={`Change ${whose}`}>
        <Avatar {...avatar} />
        <span className="avatar-edit" aria-hidden="true">
          ✎
        </span>
      </label>
      <input
        id={id}
        type="file"
        accept="image/*"
        className="visually-hidden"
        aria-label={`Change ${whose} (kept in this browser only)`}
        onChange={pick}
      />
      {avatar.photo && (
        <button type="button" className="link-button small" onClick={() => onPhoto(null)}>
          Remove photo
        </button>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </div>
  );
}
