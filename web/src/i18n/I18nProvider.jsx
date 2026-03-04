import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { MESSAGES } from "./messages";
import {
  detectPreferredLocale,
  LANGUAGES,
  LOCALE_STORAGE_KEY,
  normalizeLocale,
  RTL_LANGUAGES,
} from "./languages";

const I18nContext = createContext({
  locale: "en",
  dir: "ltr",
  languages: LANGUAGES,
  setLocale: () => {},
  t: (key) => key,
});

function getByPath(obj, path) {
  const keys = String(path || "").split(".");
  let cursor = obj;
  for (const key of keys) {
    if (!cursor || typeof cursor !== "object" || !(key in cursor)) return "";
    cursor = cursor[key];
  }
  return cursor;
}

function interpolate(template, vars) {
  if (!vars || typeof template !== "string") return template;
  return template.replace(/\{(\w+)\}/g, (_, key) => {
    const value = vars[key];
    return value === undefined || value === null ? "" : String(value);
  });
}

export function I18nProvider({ children }) {
  const [locale, setLocaleState] = useState(() => detectPreferredLocale());

  const setLocale = (nextLocale) => {
    const normalized = normalizeLocale(nextLocale) || "en";
    setLocaleState(normalized);
    try {
      window.localStorage.setItem(LOCALE_STORAGE_KEY, normalized);
    } catch {
      // ignore localStorage failures
    }
  };

  useEffect(() => {
    if (typeof document === "undefined") return;
    const dir = RTL_LANGUAGES.has(locale) ? "rtl" : "ltr";
    document.documentElement.lang = locale;
    document.documentElement.dir = dir;
    document.body.dir = dir;
  }, [locale]);

  const value = useMemo(() => {
    const dir = RTL_LANGUAGES.has(locale) ? "rtl" : "ltr";
    const dictionary = MESSAGES[locale] || MESSAGES.en;
    const fallback = MESSAGES.en;

    const t = (key, vars) => {
      const hit = getByPath(dictionary, key);
      if (typeof hit === "string") return interpolate(hit, vars);
      const fallbackHit = getByPath(fallback, key);
      if (typeof fallbackHit === "string") return interpolate(fallbackHit, vars);
      return key;
    };

    return {
      locale,
      dir,
      languages: LANGUAGES,
      setLocale,
      t,
    };
  }, [locale]);

  return <I18nContext.Provider value={value}>{children}</I18nContext.Provider>;
}

export function useI18n() {
  return useContext(I18nContext);
}
