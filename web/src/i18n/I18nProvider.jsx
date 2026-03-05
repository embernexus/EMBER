import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { MESSAGES } from "./messages";
import {
  detectPreferredLocale,
  LANGUAGES,
  LOCALE_STORAGE_KEY,
  normalizeLocale,
  RTL_LANGUAGES,
} from "./languages";

const AUTO_TRANSLATE_CACHE_PREFIX = "ember.auto_translate.";

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

function shouldSkipElement(el) {
  if (!el || !(el instanceof Element)) return true;
  if (el.closest('[data-no-auto-translate="true"]')) return true;
  if (el.closest(".mono")) return true;
  if (el.isContentEditable) return true;
  const tag = String(el.tagName || "").toUpperCase();
  return ["SCRIPT", "STYLE", "NOSCRIPT", "CODE", "PRE", "TEXTAREA", "INPUT", "SELECT", "OPTION"].includes(tag);
}

function shouldAutoTranslateText(raw) {
  const text = String(raw || "").trim();
  if (!text) return false;
  if (text.length < 2) return false;
  if (!/[A-Za-z]/.test(text)) return false;
  if (/https?:\/\//i.test(text) || /^www\./i.test(text)) return false;
  if (/^[A-Za-z0-9_-]{24,}$/.test(text)) return false;
  if (/^[\d\s.,:%+\-/$()#@!&*\[\]{}<>|\\]+$/.test(text)) return false;
  return true;
}

function parseTranslatePayload(payload, fallback) {
  if (!Array.isArray(payload) || !Array.isArray(payload[0])) return fallback;
  const segments = payload[0];
  const translated = segments
    .map((segment) => (Array.isArray(segment) ? String(segment[0] || "") : ""))
    .join("")
    .trim();
  return translated || fallback;
}

async function requestTranslation(text, locale) {
  const url = new URL("https://translate.googleapis.com/translate_a/single");
  url.searchParams.set("client", "gtx");
  url.searchParams.set("sl", "en");
  url.searchParams.set("tl", locale);
  url.searchParams.set("dt", "t");
  url.searchParams.set("q", text);

  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`translate http ${res.status}`);
  const payload = await res.json();
  return parseTranslatePayload(payload, text);
}

export function I18nProvider({ children }) {
  const [locale, setLocaleState] = useState(() => detectPreferredLocale());
  const originalTextRef = useRef(new WeakMap());
  const originalAttrRef = useRef(new WeakMap());
  const autoCacheRef = useRef({});
  const pendingRef = useRef(new Map());
  const previousLocaleRef = useRef(locale);

  const setLocale = (nextLocale) => {
    const normalized = normalizeLocale(nextLocale) || "en";
    setLocaleState(normalized);
    try {
      window.localStorage.setItem(LOCALE_STORAGE_KEY, normalized);
    } catch {
      // ignore localStorage failures
    }
  };

  const loadCacheBucket = (targetLocale) => {
    if (autoCacheRef.current[targetLocale]) return autoCacheRef.current[targetLocale];
    let bucket = {};
    try {
      const raw = window.localStorage.getItem(`${AUTO_TRANSLATE_CACHE_PREFIX}${targetLocale}`);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (parsed && typeof parsed === "object") bucket = parsed;
      }
    } catch {
      bucket = {};
    }
    autoCacheRef.current[targetLocale] = bucket;
    return bucket;
  };

  useEffect(() => {
    if (typeof document === "undefined") return;
    const dir = RTL_LANGUAGES.has(locale) ? "rtl" : "ltr";
    document.documentElement.lang = locale;
    document.documentElement.dir = dir;
    document.body.dir = dir;
  }, [locale]);

  useEffect(() => {
    if (typeof document === "undefined" || typeof window === "undefined") return;
    const root = document.body;
    if (!root) return;

    const previousLocale = previousLocaleRef.current;
    let restoreEnglishPhase = locale === "en" && previousLocale !== "en";
    previousLocaleRef.current = locale;

    let destroyed = false;
    let muting = false;
    let persistTimer = null;

    const withMute = (fn) => {
      muting = true;
      try {
        fn();
      } finally {
        muting = false;
      }
    };

    const saveCacheSoon = () => {
      if (persistTimer) window.clearTimeout(persistTimer);
      persistTimer = window.setTimeout(() => {
        try {
          const bucket = autoCacheRef.current[locale] || {};
          window.localStorage.setItem(`${AUTO_TRANSLATE_CACHE_PREFIX}${locale}`, JSON.stringify(bucket));
        } catch {
          // ignore cache persistence failures
        }
      }, 250);
    };

    const translate = async (sourceText) => {
      if (locale === "en") return sourceText;
      const bucket = loadCacheBucket(locale);
      if (bucket[sourceText]) return bucket[sourceText];
      const pendingKey = `${locale}::${sourceText}`;
      if (pendingRef.current.has(pendingKey)) return pendingRef.current.get(pendingKey);

      const p = requestTranslation(sourceText, locale)
        .then((translated) => {
          bucket[sourceText] = translated;
          saveCacheSoon();
          return translated;
        })
        .catch(() => sourceText)
        .finally(() => {
          pendingRef.current.delete(pendingKey);
        });

      pendingRef.current.set(pendingKey, p);
      return p;
    };

    const rememberOriginalText = (node) => {
      const map = originalTextRef.current;
      const current = String(node.nodeValue || "");
      if (!map.has(node)) {
        map.set(node, current);
      } else if (!muting && (locale !== "en" || !restoreEnglishPhase)) {
        const previous = String(map.get(node) || "");
        if (current && current !== previous) {
          map.set(node, current);
        }
      }
      return String(map.get(node) || "");
    };

    const rememberOriginalAttr = (el, attr) => {
      const map = originalAttrRef.current;
      let entry = map.get(el);
      if (!entry) {
        entry = {};
        map.set(el, entry);
      }
      const current = String(el.getAttribute(attr) || "");
      if (!(attr in entry)) {
        entry[attr] = current;
      } else if (!muting && (locale !== "en" || !restoreEnglishPhase) && current && current !== entry[attr]) {
        entry[attr] = current;
      }
      return String(entry[attr] || "");
    };

    const applyTextNode = (node) => {
      if (!node || node.nodeType !== Node.TEXT_NODE) return;
      const parent = node.parentElement;
      if (!parent || shouldSkipElement(parent)) return;

      const original = rememberOriginalText(node);
      if (locale === "en") {
        if (String(node.nodeValue || "") !== original) {
          withMute(() => {
            node.nodeValue = original;
          });
        }
        return;
      }

      const trimmed = original.trim();
      if (!shouldAutoTranslateText(trimmed)) {
        if (String(node.nodeValue || "") !== original) {
          withMute(() => {
            node.nodeValue = original;
          });
        }
        return;
      }

      void translate(trimmed).then((translated) => {
        if (destroyed || !node.isConnected) return;
        const latestOriginal = String(originalTextRef.current.get(node) || "");
        if (latestOriginal !== original) return;
        const lead = original.indexOf(trimmed);
        const start = lead < 0 ? 0 : lead;
        const end = start + trimmed.length;
        const nextText = `${original.slice(0, start)}${translated}${original.slice(end)}`;
        if (String(node.nodeValue || "") !== nextText) {
          withMute(() => {
            node.nodeValue = nextText;
          });
        }
      });
    };

    const applyElementAttrs = (el) => {
      if (!el || !(el instanceof Element) || shouldSkipElement(el)) return;
      const attrs = ["placeholder", "title", "aria-label"];
      for (const attr of attrs) {
        if (!el.hasAttribute(attr)) continue;
        const original = rememberOriginalAttr(el, attr);

        if (locale === "en") {
          const current = String(el.getAttribute(attr) || "");
          if (current !== original) {
            withMute(() => {
              el.setAttribute(attr, original);
            });
          }
          continue;
        }

        const trimmed = original.trim();
        if (!shouldAutoTranslateText(trimmed)) {
          const current = String(el.getAttribute(attr) || "");
          if (current !== original) {
            withMute(() => {
              el.setAttribute(attr, original);
            });
          }
          continue;
        }

        void translate(trimmed).then((translated) => {
          if (destroyed || !el.isConnected) return;
          const latest = String((originalAttrRef.current.get(el) || {})[attr] || "");
          if (latest !== original) return;
          const lead = original.indexOf(trimmed);
          const start = lead < 0 ? 0 : lead;
          const end = start + trimmed.length;
          const nextValue = `${original.slice(0, start)}${translated}${original.slice(end)}`;
          if (String(el.getAttribute(attr) || "") !== nextValue) {
            withMute(() => {
              el.setAttribute(attr, nextValue);
            });
          }
        });
      }
    };

    const scan = (node) => {
      if (!node) return;
      if (node.nodeType === Node.TEXT_NODE) {
        applyTextNode(node);
        return;
      }
      if (node.nodeType !== Node.ELEMENT_NODE) return;

      const element = node;
      applyElementAttrs(element);

      const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
      let current = walker.nextNode();
      while (current) {
        applyTextNode(current);
        current = walker.nextNode();
      }

      const withAttrs = element.querySelectorAll("[placeholder],[title],[aria-label]");
      withAttrs.forEach((el) => applyElementAttrs(el));
    };

    scan(root);
    restoreEnglishPhase = false;

    const observer = new MutationObserver((mutations) => {
      if (destroyed || muting) return;
      for (const mutation of mutations) {
        if (mutation.type === "characterData") {
          applyTextNode(mutation.target);
          continue;
        }
        if (mutation.type === "attributes") {
          applyElementAttrs(mutation.target);
          continue;
        }
        mutation.addedNodes.forEach((added) => scan(added));
      }
    });

    observer.observe(root, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
      attributeFilter: ["placeholder", "title", "aria-label"],
    });

    return () => {
      destroyed = true;
      observer.disconnect();
      if (persistTimer) window.clearTimeout(persistTimer);
    };
  }, [locale]);

  const value = useMemo(() => {
    const dir = RTL_LANGUAGES.has(locale) ? "rtl" : "ltr";
    // Keep dictionary source stable in English; non-English is auto-translated from rendered UI text.
    const dictionary = locale === "en" ? MESSAGES.en : {};
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
