import { useMemo, useState } from "react";
import { useI18n } from "../../i18n/I18nProvider";

export default function LanguageSwitcher() {
  const { locale, languages, setLocale, t, dir } = useI18n();
  const [open, setOpen] = useState(false);

  const activeLanguage = useMemo(
    () => languages.find((item) => item.code === locale) || languages[0],
    [languages, locale]
  );

  return (
    <div
      className={`lang-fab ${open ? "open" : ""}`}
      onMouseLeave={() => setOpen(false)}
      style={{ direction: dir }}
    >
      <button
        type="button"
        className="lang-fab-trigger"
        onClick={() => setOpen((prev) => !prev)}
        aria-expanded={open}
        aria-label={t("lang.select")}
      >
        <span aria-hidden="true">{"\u{1F310}"}</span>
        <span>{activeLanguage?.label || "English"}</span>
      </button>

      {open && (
        <div className="lang-fab-menu">
          <div className="lang-fab-title">{t("lang.select")}</div>
          <div className="lang-fab-list">
            {languages.map((item) => {
              const selected = item.code === locale;
              return (
                <button
                  type="button"
                  key={item.code}
                  className={`lang-fab-option ${selected ? "selected" : ""}`}
                  onClick={() => {
                    setLocale(item.code);
                    setOpen(false);
                  }}
                >
                  <span>{item.label}</span>
                  {selected ? <span>{"\u2713"}</span> : null}
                </button>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
