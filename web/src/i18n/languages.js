export const LOCALE_STORAGE_KEY = "ember.locale";

export const LANGUAGES = [
  { code: "en", label: "English" },
  { code: "es", label: "Español" },
  { code: "fr", label: "Français" },
  { code: "de", label: "Deutsch" },
  { code: "pt", label: "Português" },
  { code: "ru", label: "Русский" },
  { code: "ar", label: "العربية" },
  { code: "hi", label: "हिन्दी" },
  { code: "id", label: "Bahasa Indonesia" },
  { code: "ja", label: "日本語" },
  { code: "ko", label: "한국어" },
  { code: "zh", label: "中文" },
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
