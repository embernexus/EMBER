export const LOCALE_STORAGE_KEY = "ember.locale";

export const LANGUAGES = [
  { code: "en", label: "English" },
  { code: "es", label: "Spanish" },
  { code: "fr", label: "French" },
  { code: "de", label: "German" },
  { code: "pt", label: "Portuguese" },
  { code: "ru", label: "Russian" },
  { code: "ar", label: "Arabic" },
  { code: "hi", label: "Hindi" },
  { code: "id", label: "Indonesian" },
  { code: "ja", label: "Japanese" },
  { code: "ko", label: "Korean" },
  { code: "zh", label: "Chinese" },
];

export const RTL_LANGUAGES = new Set(["ar"]);

const SUPPORTED = new Set(LANGUAGES.map((item) => item.code));

export function normalizeLocale(raw) {
  const v = String(raw || "").trim().toLowerCase();
  if (!v) return "";
  if (SUPPORTED.has(v)) return v;
  const base = v.split("-")[0];
  if (SUPPORTED.has(base)) return base;
  if (v.startsWith("zh")) return "zh";
  if (v.startsWith("pt")) return "pt";
  return "";
}

export function detectPreferredLocale() {
  if (typeof window === "undefined") return "en";
  try {
    const saved = window.localStorage.getItem(LOCALE_STORAGE_KEY);
    const normalizedSaved = normalizeLocale(saved);
    if (normalizedSaved) return normalizedSaved;
  } catch {
    // ignore localStorage errors and fall back to navigator
  }

  const candidates = Array.isArray(window.navigator?.languages) && window.navigator.languages.length
    ? window.navigator.languages
    : [window.navigator?.language || "en"];
  for (const candidate of candidates) {
    const normalized = normalizeLocale(candidate);
    if (normalized) return normalized;
  }
  return "en";
}