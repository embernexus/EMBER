import { useEffect, useMemo, useState } from "react";
import { fmtSol } from "../lib/format";

function ToolModal({ title, onClose, children }) {
  return (
    <div
      style={{
        position: "fixed",
        inset: 0,
        zIndex: 1450,
        display: "flex",
        alignItems: "flex-start",
        justifyContent: "center",
        paddingTop: 120,
        paddingBottom: 24,
      }}
      onClick={onClose}
    >
      <div style={{ position: "absolute", inset: 0, background: "rgba(0,0,0,.74)", backdropFilter: "blur(8px)" }} />
      <div
        className="glass"
        style={{
          position: "relative",
          zIndex: 1,
          width: "min(620px,92vw)",
          maxHeight: "84vh",
          overflowY: "auto",
          padding: "22px 24px",
          border: "1px solid rgba(255,106,0,.24)",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 16 }}>
          <div style={{ fontSize: 18, fontWeight: 800, color: "#fff" }}>{title}</div>
          <button className="btn-ghost" onClick={onClose} style={{ padding: "7px 12px", fontSize: 12 }}>
            Close
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

const TOOL_HINTS = {
  holder_pooler: "One-way holder distribution that sends a fixed token amount to each recipient wallet.",
  reaction_manager: "DexScreener campaign targeting with one reaction type per run.",
  smart_sell: "Reactive managed-wallet selling with configurable triggers and timing.",
  bundle_manager: "Multi-wallet campaign funding and execution with managed or imported bundles.",
};

function defaultDraft(tool) {
  return {
    toolType: tool?.toolType || "",
    title: tool?.label || "",
    targetMint: "",
    targetUrl: "",
    simpleMode: true,
  };
}

export default function ToolsPage({
  user,
  toolsWorkspace,
  attachedTokens = [],
  loading,
  error,
  onRefresh,
  onCreateToolInstance,
  onLoadToolDetails,
  onRevealToolFundingKey,
  onRevealToolWalletKeys,
  onUpdateToolInstance,
  onRunReactionManager,
  onRefreshReactionManager,
  onRefreshToolFunding,
  onArmSmartSell,
  onPauseSmartSell,
  onReclaimSmartSell,
  onRunHolderPooler,
  onRunBundleManager,
  onReclaimBundleManager,
}) {
  const username = String(user?.username || "");
  const [draftTool, setDraftTool] = useState(null);
  const [draft, setDraft] = useState(defaultDraft({}));
  const [createError, setCreateError] = useState("");
  const [createBusy, setCreateBusy] = useState(false);
  const [copyMessage, setCopyMessage] = useState("");
  const [selectedToolId, setSelectedToolId] = useState("");
  const [toolDetails, setToolDetails] = useState(null);
  const [toolBusy, setToolBusy] = useState(false);
  const [toolError, setToolError] = useState("");
  const [fundingKeyBusy, setFundingKeyBusy] = useState(false);
  const [fundingKeyError, setFundingKeyError] = useState("");
  const [fundingKeySecret, setFundingKeySecret] = useState(null);
  const [walletKeysBusy, setWalletKeysBusy] = useState(false);
  const [walletKeysError, setWalletKeysError] = useState("");
  const [walletKeysSecret, setWalletKeysSecret] = useState(null);
  const [holderDraft, setHolderDraft] = useState({
    walletCount: 10,
    tokenAmountPerWallet: 1,
    stealthMode: true,
  });
  const [reactionDraft, setReactionDraft] = useState({
    reactionType: "rocket",
    targetCount: 1000,
  });
  const [smartSellDraft, setSmartSellDraft] = useState({
    triggerMode: "every_buy",
    sellPct: 25,
    thresholdSol: 0,
    timingMode: "randomized",
    fundingMode: "direct",
    walletCount: 6,
    walletReserveSol: 0.0015,
    stealthMode: true,
  });
  const [bundleDraft, setBundleDraft] = useState({
    walletCount: 10,
    walletMode: "managed",
    fundingMode: "direct",
    sideBias: 50,
    buySolPerWallet: 0.01,
    sellPctPerWallet: 25,
    walletReserveSol: 0.0015,
    stealthMode: true,
    importedWalletsText: "",
  });

  const catalog = Array.isArray(toolsWorkspace?.catalog) ? toolsWorkspace.catalog : [];
  const instances = Array.isArray(toolsWorkspace?.instances) ? toolsWorkspace.instances : [];
  const permissions = toolsWorkspace?.permissions || {};
  const attachedMintOptions = useMemo(
    () =>
      (Array.isArray(attachedTokens) ? attachedTokens : [])
        .filter((token) => String(token?.mint || "").trim())
        .map((token) => ({
          id: String(token?.id || ""),
          mint: String(token?.mint || "").trim(),
          symbol: String(token?.symbol || token?.name || "TOKEN").trim(),
        })),
    [attachedTokens]
  );

  const groupedInstances = useMemo(() => {
    const groups = new Map();
    for (const instance of instances) {
      const key = String(instance.toolType || "");
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(instance);
    }
    return groups;
  }, [instances]);

  useEffect(() => {
    if (!selectedToolId || typeof onLoadToolDetails !== "function") return;
    let active = true;
    setToolBusy(true);
    setToolError("");
    onLoadToolDetails(selectedToolId)
      .then((data) => {
        if (!active) return;
        setToolDetails(data);
        if (data?.tool?.toolType === "holder_pooler") {
          const config = data.tool.config || {};
          setHolderDraft({
            walletCount: Number(config.walletCount || 10),
            tokenAmountPerWallet: Number(config.tokenAmountPerWallet || 1),
            stealthMode: Boolean(config.stealthMode),
          });
        } else if (data?.tool?.toolType === "reaction_manager") {
          const config = data.tool.config || {};
          setReactionDraft({
            reactionType: String(config.reactionType || "rocket"),
            targetCount: Number(config.targetCount || 1000),
          });
        } else if (data?.tool?.toolType === "smart_sell") {
          const config = data.tool.config || {};
          setSmartSellDraft({
            triggerMode: String(config.triggerMode || "every_buy"),
            sellPct: Number(config.sellPct ?? 25),
            thresholdSol: Number(config.thresholdSol || 0),
            timingMode: String(config.timingMode || "randomized"),
            fundingMode: String(config.fundingMode || "direct"),
            walletCount: Number(config.walletCount || 6),
            walletReserveSol: Number(config.walletReserveSol || 0.0015),
            stealthMode: Boolean(config.stealthMode),
          });
        } else if (data?.tool?.toolType === "bundle_manager") {
          const config = data.tool.config || {};
          setBundleDraft({
            walletCount: Number(config.walletCount || 10),
            walletMode: String(config.walletMode || "managed"),
            fundingMode: String(config.fundingMode || "direct"),
            sideBias: Number(config.sideBias ?? 50),
            buySolPerWallet: Number(config.buySolPerWallet || 0.01),
            sellPctPerWallet: Number(config.sellPctPerWallet ?? 25),
            walletReserveSol: Number(config.walletReserveSol || 0.0015),
            stealthMode: Boolean(config.stealthMode),
            importedWalletsText: "",
          });
        }
      })
      .catch((err) => {
        if (!active) return;
        setToolError(err?.message || "Unable to load tool details.");
      })
      .finally(() => {
        if (!active) return;
        setToolBusy(false);
      });
    return () => {
      active = false;
    };
  }, [selectedToolId, onLoadToolDetails]);

  const openCreate = (tool) => {
    setDraftTool(tool);
    setDraft(defaultDraft(tool));
    setCreateError("");
  };

  const createInstance = async () => {
    if (!draftTool || typeof onCreateToolInstance !== "function") return;
    setCreateBusy(true);
    setCreateError("");
    try {
      await onCreateToolInstance({
        toolType: draftTool.toolType,
        title: draft.title,
        targetMint: draftTool.targetKind === "mint" ? draft.targetMint : "",
        targetUrl: draftTool.targetKind === "url" ? draft.targetUrl : "",
        simpleMode: draft.simpleMode,
      });
      setDraftTool(null);
      setDraft(defaultDraft({}));
    } catch (err) {
      setCreateError(err?.message || "Unable to create tool instance.");
    } finally {
      setCreateBusy(false);
    }
  };

  const copyText = async (value, label) => {
    try {
      await navigator.clipboard.writeText(String(value || ""));
      setCopyMessage(`${label} copied.`);
      window.setTimeout(() => setCopyMessage(""), 1800);
    } catch {
      setCopyMessage(`Unable to copy ${label.toLowerCase()}.`);
      window.setTimeout(() => setCopyMessage(""), 1800);
    }
  };

  const selectedInstance = selectedToolId
    ? instances.find((instance) => instance.id === selectedToolId) || null
    : null;

  const saveHolderSettings = async () => {
    if (!selectedToolId || typeof onUpdateToolInstance !== "function") return;
    setToolBusy(true);
    setToolError("");
    try {
      const data = await onUpdateToolInstance(selectedToolId, {
        simpleMode: Boolean(toolDetails?.tool?.simpleMode),
        title: toolDetails?.tool?.title || selectedInstance?.title || "",
        config: holderDraft,
      });
      setToolDetails(data);
    } catch (err) {
      setToolError(err?.message || "Unable to save Holder Pooler settings.");
    } finally {
      setToolBusy(false);
    }
  };

  const runToolAction = async (fn) => {
    if (!selectedToolId || typeof fn !== "function") return;
    setToolBusy(true);
    setToolError("");
    try {
      const data = await fn(selectedToolId);
      setToolDetails(data);
    } catch (err) {
      setToolError(err?.message || "Tool action failed.");
    } finally {
      setToolBusy(false);
    }
  };

  const confirmAndRunToolAction = async (message, fn) => {
    if (typeof window !== "undefined" && !window.confirm(message)) return;
    await runToolAction(fn);
  };

  const revealFundingKey = async () => {
    if (!selectedToolId || typeof onRevealToolFundingKey !== "function") return;
    setFundingKeyBusy(true);
    setFundingKeyError("");
    try {
      const data = await onRevealToolFundingKey(selectedToolId);
      setFundingKeySecret(data);
    } catch (err) {
      setFundingKeyError(err?.message || "Unable to reveal funding key.");
    } finally {
      setFundingKeyBusy(false);
    }
  };

  const revealWalletKeys = async () => {
    if (!selectedToolId || typeof onRevealToolWalletKeys !== "function") return;
    setWalletKeysBusy(true);
    setWalletKeysError("");
    try {
      const data = await onRevealToolWalletKeys(selectedToolId);
      setWalletKeysSecret(data);
    } catch (err) {
      setWalletKeysError(err?.message || "Unable to reveal tool wallet keys.");
    } finally {
      setWalletKeysBusy(false);
    }
  };

  const saveBundleSettings = async () => {
    if (!selectedToolId || typeof onUpdateToolInstance !== "function") return;
    setToolBusy(true);
    setToolError("");
    try {
      const importedWallets = bundleDraft.importedWalletsText
        .split(/[\n,\r]+/)
        .map((value) => value.trim())
        .filter(Boolean);
      const data = await onUpdateToolInstance(selectedToolId, {
        simpleMode: Boolean(toolDetails?.tool?.simpleMode),
        title: toolDetails?.tool?.title || selectedInstance?.title || "",
        config: {
          walletCount: bundleDraft.walletCount,
          walletMode: bundleDraft.walletMode,
          fundingMode: bundleDraft.fundingMode,
          sideBias: bundleDraft.sideBias,
          buySolPerWallet: bundleDraft.buySolPerWallet,
          sellPctPerWallet: bundleDraft.sellPctPerWallet,
          walletReserveSol: bundleDraft.walletReserveSol,
          stealthMode: bundleDraft.fundingMode === "ember",
          importedWallets,
        },
      });
      setToolDetails(data);
      setBundleDraft((prev) => ({ ...prev, importedWalletsText: "" }));
    } catch (err) {
      setToolError(err?.message || "Unable to save Bundle Manager settings.");
    } finally {
      setToolBusy(false);
    }
  };

  const saveReactionSettings = async () => {
    if (!selectedToolId || typeof onUpdateToolInstance !== "function") return;
    setToolBusy(true);
    setToolError("");
    try {
      const data = await onUpdateToolInstance(selectedToolId, {
        simpleMode: Boolean(toolDetails?.tool?.simpleMode),
        title: toolDetails?.tool?.title || selectedInstance?.title || "",
        config: reactionDraft,
      });
      setToolDetails(data);
    } catch (err) {
      setToolError(err?.message || "Unable to save Reaction Manager settings.");
    } finally {
      setToolBusy(false);
    }
  };

  const saveSmartSellSettings = async () => {
    if (!selectedToolId || typeof onUpdateToolInstance !== "function") return;
    setToolBusy(true);
    setToolError("");
    try {
      const data = await onUpdateToolInstance(selectedToolId, {
        simpleMode: Boolean(toolDetails?.tool?.simpleMode),
        title: toolDetails?.tool?.title || selectedInstance?.title || "",
        config: smartSellDraft,
      });
      setToolDetails(data);
    } catch (err) {
      setToolError(err?.message || "Unable to save Smart Sell settings.");
    } finally {
      setToolBusy(false);
    }
  };

  return (
    <div style={{ position: "relative", zIndex: 3, maxWidth: 1300, margin: "0 auto", padding: "18px 24px 80px" }}>
      <section className="glass" style={{ padding: "22px 24px", marginBottom: 18 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 16, alignItems: "flex-start", flexWrap: "wrap" }}>
          <div>
            <div style={{ fontSize: 30, fontWeight: 900, color: "#fff", letterSpacing: 0.4 }}>Tools</div>
            <div style={{ color: "rgba(255,255,255,.62)", fontSize: 13, lineHeight: 1.7, maxWidth: 760, marginTop: 8 }}>
              One-time paid tool instances with their own funding wallet, activity log, and control surface. Each instance is reusable after unlock for the same target.
            </div>
            <div style={{ color: "rgba(255,255,255,.4)", fontSize: 12, marginTop: 8 }}>
              Logged in as {username || "unknown"} {permissions?.role ? `(${permissions.role})` : ""}
            </div>
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            <button className="btn-ghost" onClick={onRefresh} disabled={loading} style={{ padding: "10px 16px", fontSize: 12 }}>
              {loading ? "Refreshing..." : "Refresh"}
            </button>
          </div>
        </div>
        {error && <div style={{ color: "#ff8b8b", fontSize: 12, marginTop: 12 }}>{error}</div>}
        {copyMessage && <div style={{ color: "#8fffc8", fontSize: 12, marginTop: 12 }}>{copyMessage}</div>}
      </section>

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px,1fr))", gap: 14 }}>
        {catalog.map((tool) => {
          const count = groupedInstances.get(tool.toolType)?.length || 0;
          return (
            <div key={tool.toolType} className="glass" style={{ padding: "18px 18px 16px" }}>
              <div style={{ display: "flex", justifyContent: "space-between", gap: 10, alignItems: "flex-start" }}>
                <div>
                  <div style={{ color: "#fff", fontSize: 18, fontWeight: 800 }}>{tool.label}</div>
                  <div style={{ color: "rgba(255,255,255,.56)", fontSize: 12, marginTop: 6, lineHeight: 1.6 }}>
                    {TOOL_HINTS[tool.toolType] || tool.description}
                  </div>
                </div>
                <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.42)", letterSpacing: 0.8 }}>
                  {count} ACTIVE
                </div>
              </div>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10, marginTop: 14 }}>
                <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                  <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>UNLOCK</div>
                  <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", marginTop: 6 }}>{fmtSol(tool.unlockFeeSol)} SOL</div>
                </div>
                <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                  <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>RESERVE</div>
                  <div style={{ fontSize: 16, fontWeight: 800, color: "#fff", marginTop: 6 }}>{fmtSol(tool.reserveSol)} SOL</div>
                </div>
              </div>
              <div style={{ color: "rgba(255,255,255,.48)", fontSize: 11, lineHeight: 1.6, marginTop: 10 }}>
                Fund {fmtSol(tool.requiredSol)} SOL total to unlock and keep the instance operational.
                {tool.runtimeFeeLamports > 0 ? ` Runtime: ${fmtSol(tool.runtimeFeeSol)} SOL per ${tool.runtimeFeeWindowHours}h window.` : ""}
              </div>
              <button className="btn-fire" onClick={() => openCreate(tool)} style={{ width: "100%", marginTop: 14, justifyContent: "center", padding: "11px 14px", fontSize: 12 }}>
                Create {tool.label}
              </button>
            </div>
          );
        })}
      </section>

      <section className="glass" style={{ padding: "20px 22px", marginTop: 18 }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", marginBottom: 14 }}>
          <div style={{ fontSize: 18, fontWeight: 800, color: "#fff" }}>Tool Instances</div>
          <div style={{ color: "rgba(255,255,255,.45)", fontSize: 12 }}>{instances.length} total</div>
        </div>
        {instances.length === 0 ? (
          <div style={{ color: "rgba(255,255,255,.48)", fontSize: 13, lineHeight: 1.7 }}>
            No tool instances yet. Create one above and fund its wallet to unlock it.
          </div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {instances.map((instance) => (
              <div key={instance.id} style={{ border: "1px solid rgba(255,255,255,.06)", borderRadius: 16, padding: 16, background: "rgba(255,255,255,.02)" }}>
                <div style={{ display: "flex", justifyContent: "space-between", gap: 14, alignItems: "flex-start", flexWrap: "wrap" }}>
                  <div>
                    <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap" }}>
                      <div style={{ color: "#fff", fontSize: 16, fontWeight: 800 }}>{instance.title || instance.label}</div>
                      <span style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,214,184,.88)", background: "rgba(255,106,0,.14)", borderRadius: 999, padding: "4px 8px" }}>
                        {instance.status.replace(/_/g, " ").toUpperCase()}
                      </span>
                    </div>
                    <div style={{ color: "rgba(255,255,255,.48)", fontSize: 12, marginTop: 6 }}>
                      {instance.label} {instance.targetMint ? `• ${instance.targetMint}` : instance.targetUrl ? `• ${instance.targetUrl}` : ""}
                    </div>
                  </div>
                  <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10, minWidth: 320 }}>
                    <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                      <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>FUNDED</div>
                      <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>{fmtSol(instance.balanceSol)} SOL</div>
                    </div>
                    <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                      <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>REQUIRED</div>
                      <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>{fmtSol(instance.requiredSol)} SOL</div>
                    </div>
                    <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                      <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>MODE</div>
                      <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>{instance.simpleMode ? "Simple" : "Advanced"}</div>
                    </div>
                  </div>
                </div>

                <div style={{ display: "flex", gap: 10, alignItems: "center", flexWrap: "wrap", marginTop: 12 }}>
                  <div style={{ color: "rgba(255,255,255,.48)", fontSize: 11 }}>Funding wallet:</div>
                  <code style={{ color: "#fff", fontSize: 12, wordBreak: "break-all" }}>{instance.fundingWalletPubkey}</code>
                  <button className="btn-ghost" onClick={() => copyText(instance.fundingWalletPubkey, "Funding wallet")} style={{ padding: "7px 10px", fontSize: 11 }}>
                    Copy
                  </button>
                </div>
                {instance.runtimeFeeLamports > 0 && (
                  <div style={{ color: "rgba(255,255,255,.45)", fontSize: 11, marginTop: 10 }}>
                    Runtime fee: {fmtSol(instance.runtimeFeeSol)} SOL every {instance.runtimeFeeWindowHours} hours while active.
                  </div>
                )}
                <div style={{ marginTop: 12 }}>
                  <button
                    className="btn-ghost"
                    onClick={() => setSelectedToolId(instance.id)}
                    style={{ padding: "8px 12px", fontSize: 11 }}
                  >
                    Open Workspace
                  </button>
                </div>
                {instance.lastError && (
                  <div style={{ color: "#ff9c9c", fontSize: 11, marginTop: 10 }}>Last error: {instance.lastError}</div>
                )}
                <div style={{ marginTop: 14 }}>
                  <div style={{ color: "rgba(255,255,255,.58)", fontSize: 11, fontWeight: 800, letterSpacing: 0.8, marginBottom: 8 }}>RECENT LOGS</div>
                  {instance.events?.length ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                      {instance.events.slice(0, 4).map((event) => (
                        <div key={event.id} style={{ fontSize: 12, color: "rgba(255,255,255,.68)", lineHeight: 1.6 }}>
                          <span style={{ color: "#fff" }}>{event.message}</span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "rgba(255,255,255,.4)", fontSize: 12 }}>No tool logs yet.</div>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {selectedInstance && (
        <ToolModal
          title={`${selectedInstance.label} Workspace`}
          onClose={() => {
            setSelectedToolId("");
            setFundingKeySecret(null);
            setFundingKeyError("");
            setWalletKeysSecret(null);
            setWalletKeysError("");
          }}
        >
          {toolBusy && <div style={{ color: "rgba(255,255,255,.55)", fontSize: 12, marginBottom: 12 }}>Loading...</div>}
          {toolError && <div style={{ color: "#ff9c9c", fontSize: 12, marginBottom: 12 }}>{toolError}</div>}
          {fundingKeyError && <div style={{ color: "#ff9c9c", fontSize: 12, marginBottom: 12 }}>{fundingKeyError}</div>}
          {walletKeysError && <div style={{ color: "#ff9c9c", fontSize: 12, marginBottom: 12 }}>{walletKeysError}</div>}
          {toolDetails?.tool && (
            <>
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0,1fr))", gap: 10 }}>
                <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                  <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>STATUS</div>
                  <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>
                    {toolDetails.tool.status.replace(/_/g, " ")}
                  </div>
                </div>
                <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                  <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>FUNDING SOL</div>
                  <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>
                    {fmtSol(toolDetails.funding?.solBalance || 0)} SOL
                  </div>
                </div>
                <div style={{ padding: "10px 12px", borderRadius: 12, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
                  <div style={{ fontSize: 10, fontWeight: 800, color: "rgba(255,255,255,.35)", letterSpacing: 0.8 }}>FUNDING TOKENS</div>
                  <div style={{ fontSize: 14, fontWeight: 800, color: "#fff", marginTop: 6 }}>
                    {Number(toolDetails.funding?.tokenBalance || 0).toFixed(6)}
                  </div>
                </div>
              </div>

              <div
                style={{
                  marginTop: 14,
                  padding: "12px 14px",
                  borderRadius: 14,
                  border: "1px solid rgba(255,255,255,.06)",
                  background: "rgba(255,255,255,.02)",
                }}
              >
                <div style={{ color: "rgba(255,255,255,.35)", fontSize: 10, fontWeight: 800, letterSpacing: 0.8, marginBottom: 8 }}>
                  FUNDING WALLET
                </div>
                <div style={{ color: "rgba(255,255,255,.78)", fontSize: 12, lineHeight: 1.6, wordBreak: "break-all" }}>
                  <code style={{ color: "#fff" }}>{toolDetails.funding?.walletPubkey || toolDetails.tool.fundingWalletPubkey}</code>
                </div>
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 10 }}>
                  <button
                    className="btn-ghost"
                    onClick={() => copyText(toolDetails.funding?.walletPubkey || toolDetails.tool.fundingWalletPubkey, "Funding wallet")}
                    style={{ padding: "8px 12px", fontSize: 11 }}
                  >
                    Copy Wallet
                  </button>
                  <a
                    className="btn-ghost"
                    href={`https://solscan.io/account/${encodeURIComponent(toolDetails.funding?.walletPubkey || toolDetails.tool.fundingWalletPubkey)}`}
                    target="_blank"
                    rel="noreferrer"
                    style={{ padding: "8px 12px", fontSize: 11, textDecoration: "none" }}
                  >
                    View on Solscan
                  </a>
                  {toolDetails.permissions?.isOwner ? (
                    <button
                      className="btn-ghost"
                      onClick={revealFundingKey}
                      disabled={fundingKeyBusy || typeof onRevealToolFundingKey !== "function"}
                      style={{ padding: "8px 12px", fontSize: 11 }}
                    >
                      {fundingKeyBusy ? "Loading Key..." : "Show Funding Key"}
                    </button>
                  ) : null}
                  {toolDetails.permissions?.isOwner && Array.isArray(toolDetails.wallets) && toolDetails.wallets.length ? (
                    <button
                      className="btn-ghost"
                      onClick={revealWalletKeys}
                      disabled={walletKeysBusy || typeof onRevealToolWalletKeys !== "function"}
                      style={{ padding: "8px 12px", fontSize: 11 }}
                    >
                      {walletKeysBusy ? "Loading Wallet Keys..." : "Show Wallet Keys"}
                    </button>
                  ) : null}
                </div>
              </div>

              {toolDetails.tool.fundingState?.mode ? (
                <div
                  style={{
                    marginTop: 12,
                    padding: "12px 14px",
                    borderRadius: 14,
                    border: "1px solid rgba(255,255,255,.06)",
                    background: "rgba(255,255,255,.02)",
                  }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                    <div>
                      <div style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>
                        {toolDetails.tool.fundingState.modeLabel || "Funding"}
                      </div>
                      <div style={{ color: "rgba(255,255,255,.58)", fontSize: 12, marginTop: 4, lineHeight: 1.6 }}>
                        {toolDetails.tool.fundingState.statusText || "Ready"}
                        {toolDetails.tool.fundingState.orderId ? ` \u2022 Order ${toolDetails.tool.fundingState.orderId}` : ""}
                        {toolDetails.tool.fundingState.depositAmountSol
                          ? ` \u2022 ${fmtSol(toolDetails.tool.fundingState.depositAmountSol)} SOL`
                          : ""}
                        {toolDetails.tool.fundingState.sentSol
                          ? ` \u2022 Sent ${fmtSol(toolDetails.tool.fundingState.sentSol)} SOL`
                          : ""}
                        {toolDetails.tool.fundingState.recipientCount
                          ? ` \u2022 ${Number(toolDetails.tool.fundingState.recipientCount)} recipients`
                          : ""}
                      </div>
                    </div>
                    {toolDetails.tool.fundingState.mode === "ember" && toolDetails.tool.fundingState.orderId ? (
                      <button
                        className="btn-ghost"
                        onClick={() => runToolAction(onRefreshToolFunding)}
                        disabled={toolBusy || typeof onRefreshToolFunding !== "function"}
                        style={{ padding: "9px 12px", fontSize: 11 }}
                      >
                        Refresh EMBER Funding
                      </button>
                    ) : null}
                  </div>
                  {toolDetails.tool.fundingState.depositWalletAddress ? (
                    <div style={{ marginTop: 10, color: "rgba(255,255,255,.68)", fontSize: 12, lineHeight: 1.6 }}>
                      EMBER deposit wallet:{" "}
                      <code style={{ color: "#fff", wordBreak: "break-all" }}>{toolDetails.tool.fundingState.depositWalletAddress}</code>
                    </div>
                  ) : null}
                  {toolDetails.tool.fundingState.depositWalletAddress ? (
                    <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 10 }}>
                      <button
                        className="btn-ghost"
                        onClick={() => copyText(toolDetails.tool.fundingState.depositWalletAddress, "EMBER deposit wallet")}
                        style={{ padding: "8px 12px", fontSize: 11 }}
                      >
                        Copy EMBER Deposit
                      </button>
                      <a
                        className="btn-ghost"
                        href={`https://solscan.io/account/${encodeURIComponent(toolDetails.tool.fundingState.depositWalletAddress)}`}
                        target="_blank"
                        rel="noreferrer"
                        style={{ padding: "8px 12px", fontSize: 11, textDecoration: "none" }}
                      >
                        View EMBER Deposit
                      </a>
                    </div>
                  ) : null}
                </div>
              ) : null}

              {toolDetails.tool.toolType === "holder_pooler" && (
                <>
                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Holder Pooler Settings</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>WALLET COUNT</label>
                      <input className="input-f" type="number" min="1" max="50" value={holderDraft.walletCount} onChange={(e) => setHolderDraft((prev) => ({ ...prev, walletCount: Number(e.target.value || 1) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TOKEN PER WALLET</label>
                      <input className="input-f" type="number" min="0" step="0.000001" value={holderDraft.tokenAmountPerWallet} onChange={(e) => setHolderDraft((prev) => ({ ...prev, tokenAmountPerWallet: Number(e.target.value || 0) }))} />
                    </div>
                    <div style={{ gridColumn: "1 / -1", display: "flex", gap: 14, flexWrap: "wrap", alignItems: "center" }}>
                      <label style={{ color: "#fff", fontSize: 12, display: "inline-flex", gap: 8, alignItems: "center" }}>
                        <input type="checkbox" checked={holderDraft.stealthMode} onChange={(e) => setHolderDraft((prev) => ({ ...prev, stealthMode: e.target.checked }))} />
                        Stealth wallet mode
                      </label>
                    </div>
                  </div>

                  <div style={{ marginTop: 12, color: "rgba(255,255,255,.58)", fontSize: 12, lineHeight: 1.7 }}>
                    Holder Pooler is one-way. Each recipient wallet receives the configured token amount and is not reclaimable later.
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 16 }}>
                    <button className="btn-fire" onClick={saveHolderSettings} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Save Settings
                    </button>
                    <button
                      className="btn-fire"
                      onClick={() =>
                        confirmAndRunToolAction(
                          "Holder Pooler is one-way. The recipient wallets are not reclaimable later. Continue?",
                          onRunHolderPooler
                        )
                      }
                      disabled={toolBusy}
                      style={{ padding: "10px 14px", fontSize: 12 }}
                    >
                      Run Distribution
                    </button>
                  </div>

                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Managed Wallets</div>
                  {Array.isArray(toolDetails.wallets) && toolDetails.wallets.length ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 10 }}>
                      {toolDetails.wallets.map((wallet) => (
                        <div key={wallet.id} style={{ border: "1px solid rgba(255,255,255,.06)", borderRadius: 12, padding: 12, background: "rgba(255,255,255,.02)" }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                            <div>
                              <div style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>{wallet.label}</div>
                              <div style={{ color: "rgba(255,255,255,.5)", fontSize: 11, marginTop: 4 }}>{wallet.walletPubkey}</div>
                            </div>
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                              <div style={{ color: "#fff", fontSize: 12 }}>{fmtSol(wallet.solBalance)} SOL</div>
                              <div style={{ color: "#fff", fontSize: 12 }}>{Number(wallet.tokenBalance || 0).toFixed(6)} TOK</div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "rgba(255,255,255,.45)", fontSize: 12, marginTop: 10 }}>No managed wallets created yet.</div>
                  )}
                </>
              )}

              {toolDetails.tool.toolType === "reaction_manager" && (
                <>
                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Reaction Manager Settings</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>REACTION TYPE</label>
                      <select className="input-f" value={reactionDraft.reactionType} onChange={(e) => setReactionDraft((prev) => ({ ...prev, reactionType: e.target.value }))}>
                        <option value="rocket">Rocket</option>
                        <option value="fire">Fire</option>
                        <option value="poop">Poop</option>
                        <option value="broken_heart">Broken Heart</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TARGET COUNT</label>
                      <input className="input-f" type="number" min="1000" max="500000" step="1000" value={reactionDraft.targetCount} onChange={(e) => setReactionDraft((prev) => ({ ...prev, targetCount: Number(e.target.value || 1000) }))} />
                    </div>
                  </div>

                  <div style={{ marginTop: 12, color: "rgba(255,255,255,.55)", fontSize: 12, lineHeight: 1.7 }}>
                    Target link: <code style={{ color: "#fff" }}>{toolDetails.tool.targetUrl}</code>
                    <br />
                    Order id: <span style={{ color: "#fff" }}>{toolDetails.tool.state?.reactionOrderId || "Not started"}</span>
                    <br />
                    Remaining: <span style={{ color: "#fff" }}>{Number(toolDetails.tool.state?.reactionRemains || 0)}</span>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 16 }}>
                    <button className="btn-fire" onClick={saveReactionSettings} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Save Settings
                    </button>
                    <button className="btn-fire" onClick={() => runToolAction(onRunReactionManager)} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Start Campaign
                    </button>
                    <button className="btn-ghost" onClick={() => runToolAction(onRefreshReactionManager)} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Refresh Status
                    </button>
                  </div>
                </>
              )}

              {toolDetails.tool.toolType === "smart_sell" && (
                <>
                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Smart Sell Settings</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TRIGGER MODE</label>
                      <select className="input-f" value={smartSellDraft.triggerMode} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, triggerMode: e.target.value }))}>
                        <option value="every_buy">Every Buy</option>
                        <option value="threshold">Buys Above Threshold</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>SELL % PER BUY</label>
                      <input className="input-f" type="number" min="1" max="100" step="1" value={smartSellDraft.sellPct} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, sellPct: Number(e.target.value || 25) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>BUY THRESHOLD SOL</label>
                      <input className="input-f" type="number" min="0" step="0.000001" value={smartSellDraft.thresholdSol} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, thresholdSol: Number(e.target.value || 0) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TIMING</label>
                      <select className="input-f" value={smartSellDraft.timingMode} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, timingMode: e.target.value }))}>
                        <option value="instant">Instant</option>
                        <option value="randomized">Randomized Delay</option>
                        <option value="split">Split Window</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>FUNDING METHOD</label>
                      <select className="input-f" value={smartSellDraft.fundingMode} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, fundingMode: e.target.value }))}>
                        <option value="direct">Direct Funding</option>
                        <option value="ember">EMBER Funding</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>WALLET COUNT</label>
                      <input className="input-f" type="number" min="1" max="50" value={smartSellDraft.walletCount} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, walletCount: Number(e.target.value || 6) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>RESERVE PER WALLET</label>
                      <input className="input-f" type="number" min="0.0005" step="0.0001" value={smartSellDraft.walletReserveSol} onChange={(e) => setSmartSellDraft((prev) => ({ ...prev, walletReserveSol: Number(e.target.value || 0.0015) }))} />
                    </div>
                    <div style={{ gridColumn: "1 / -1", color: "rgba(255,255,255,.55)", fontSize: 12, lineHeight: 1.6 }}>
                      Direct Funding sends reserve SOL straight to the managed sell wallets. EMBER Funding routes those top-ups through the EMBER funding network first.
                    </div>
                  </div>

                  <div style={{ marginTop: 12, color: "rgba(255,255,255,.55)", fontSize: 12, lineHeight: 1.7 }}>
                    Pending signals: <span style={{ color: "#fff" }}>{Array.isArray(toolDetails.tool.state?.pendingSignals) ? toolDetails.tool.state.pendingSignals.length : 0}</span>
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 16 }}>
                    <button className="btn-fire" onClick={saveSmartSellSettings} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Save Settings
                    </button>
                    <button className="btn-fire" onClick={() => runToolAction(onArmSmartSell)} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Arm Smart Sell
                    </button>
                    <button className="btn-ghost" onClick={() => runToolAction(onPauseSmartSell)} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Pause
                    </button>
                    <button
                      className="btn-ghost"
                      onClick={() =>
                        confirmAndRunToolAction(
                          "Reclaiming Smart Sell will move managed wallet balances back into the funding wallet. Continue?",
                          onReclaimSmartSell
                        )
                      }
                      disabled={toolBusy}
                      style={{ padding: "10px 14px", fontSize: 12 }}
                    >
                      Reclaim Back
                    </button>
                  </div>

                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Sell Wallets</div>
                  {Array.isArray(toolDetails.wallets) && toolDetails.wallets.length ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 10 }}>
                      {toolDetails.wallets.map((wallet) => (
                        <div key={wallet.id} style={{ border: "1px solid rgba(255,255,255,.06)", borderRadius: 12, padding: 12, background: "rgba(255,255,255,.02)" }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                            <div>
                              <div style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>{wallet.label}</div>
                              <div style={{ color: "rgba(255,255,255,.5)", fontSize: 11, marginTop: 4 }}>{wallet.walletPubkey}</div>
                            </div>
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                              <div style={{ color: "#fff", fontSize: 12 }}>{fmtSol(wallet.solBalance)} SOL</div>
                              <div style={{ color: "#fff", fontSize: 12 }}>{Number(wallet.tokenBalance || 0).toFixed(6)} TOK</div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "rgba(255,255,255,.45)", fontSize: 12, marginTop: 10 }}>No Smart Sell wallets created yet.</div>
                  )}
                </>
              )}

              {toolDetails.tool.toolType === "bundle_manager" && (
                <>
                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Bundle Manager Settings</div>
                  <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 12 }}>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>WALLET COUNT</label>
                      <input className="input-f" type="number" min="1" max="50" value={bundleDraft.walletCount} onChange={(e) => setBundleDraft((prev) => ({ ...prev, walletCount: Number(e.target.value || 1) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>WALLET MODE</label>
                      <select className="input-f" value={bundleDraft.walletMode} onChange={(e) => setBundleDraft((prev) => ({ ...prev, walletMode: e.target.value }))}>
                        <option value="managed">Managed Wallets</option>
                        <option value="imported">Imported Wallets</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>FUNDING METHOD</label>
                      <select className="input-f" value={bundleDraft.fundingMode} onChange={(e) => setBundleDraft((prev) => ({ ...prev, fundingMode: e.target.value }))}>
                        <option value="direct">Direct Funding</option>
                        <option value="ember">EMBER Funding</option>
                      </select>
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>BUY SOL PER BUY WALLET</label>
                      <input className="input-f" type="number" min="0" step="0.000001" value={bundleDraft.buySolPerWallet} onChange={(e) => setBundleDraft((prev) => ({ ...prev, buySolPerWallet: Number(e.target.value || 0) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>SELL % PER SELL WALLET</label>
                      <input className="input-f" type="number" min="1" max="100" step="1" value={bundleDraft.sellPctPerWallet} onChange={(e) => setBundleDraft((prev) => ({ ...prev, sellPctPerWallet: Number(e.target.value || 25) }))} />
                    </div>
                    <div>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>RESERVE PER WALLET</label>
                      <input className="input-f" type="number" min="0.0005" step="0.0001" value={bundleDraft.walletReserveSol} onChange={(e) => setBundleDraft((prev) => ({ ...prev, walletReserveSol: Number(e.target.value || 0.0015) }))} />
                    </div>
                    <div style={{ gridColumn: "1 / -1" }}>
                      <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 8 }}>
                        SIDE BIAS ({bundleDraft.sideBias}% buy / {100 - bundleDraft.sideBias}% sell)
                      </label>
                      <input
                        type="range"
                        min="0"
                        max="100"
                        step="1"
                        value={bundleDraft.sideBias}
                        onChange={(e) => setBundleDraft((prev) => ({ ...prev, sideBias: Number(e.target.value || 50) }))}
                        style={{ width: "100%" }}
                      />
                    </div>
                    <div style={{ gridColumn: "1 / -1", color: "rgba(255,255,255,.55)", fontSize: 12, lineHeight: 1.6 }}>
                      Direct Funding tops up bundle wallets from the master wallet. EMBER Funding routes bundle top-ups through the EMBER funding network before the campaign runs.
                    </div>
                    {bundleDraft.walletMode === "imported" && (
                      <div style={{ gridColumn: "1 / -1" }}>
                        <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>IMPORTED PRIVATE KEYS</label>
                        <textarea
                          className="input-f"
                          rows={6}
                          value={bundleDraft.importedWalletsText}
                          onChange={(e) => setBundleDraft((prev) => ({ ...prev, importedWalletsText: e.target.value }))}
                          placeholder="Paste one base58 private key per line. Saving adds them to this bundle."
                          style={{ resize: "vertical" }}
                        />
                        <div style={{ color: "rgba(255,255,255,.45)", fontSize: 11, marginTop: 6 }}>
                          Imported wallet adds are additive. Existing imported wallets remain attached unless reclaimed.
                        </div>
                      </div>
                    )}
                  </div>

                  <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 16 }}>
                    <button className="btn-fire" onClick={saveBundleSettings} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Save Settings
                    </button>
                    <button className="btn-fire" onClick={() => runToolAction(onRunBundleManager)} disabled={toolBusy} style={{ padding: "10px 14px", fontSize: 12 }}>
                      Run Campaign
                    </button>
                    <button
                      className="btn-ghost"
                      onClick={() =>
                        confirmAndRunToolAction(
                          "Reclaiming Bundle Manager will move managed wallet balances back into the funding wallet. Imported wallets remain external. Continue?",
                          onReclaimBundleManager
                        )
                      }
                      disabled={toolBusy}
                      style={{ padding: "10px 14px", fontSize: 12 }}
                    >
                      Reclaim Back
                    </button>
                  </div>

                  <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Bundle Wallets</div>
                  {Array.isArray(toolDetails.wallets) && toolDetails.wallets.length ? (
                    <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 10 }}>
                      {toolDetails.wallets.map((wallet) => (
                        <div key={wallet.id} style={{ border: "1px solid rgba(255,255,255,.06)", borderRadius: 12, padding: 12, background: "rgba(255,255,255,.02)" }}>
                          <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center", flexWrap: "wrap" }}>
                            <div>
                              <div style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>
                                {wallet.label} {wallet.imported ? <span style={{ color: "rgba(255,214,184,.88)", fontSize: 11 }}>(Imported)</span> : null}
                              </div>
                              <div style={{ color: "rgba(255,255,255,.5)", fontSize: 11, marginTop: 4 }}>{wallet.walletPubkey}</div>
                            </div>
                            <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
                              <div style={{ color: "#fff", fontSize: 12 }}>{fmtSol(wallet.solBalance)} SOL</div>
                              <div style={{ color: "#fff", fontSize: 12 }}>{Number(wallet.tokenBalance || 0).toFixed(6)} TOK</div>
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <div style={{ color: "rgba(255,255,255,.45)", fontSize: 12, marginTop: 10 }}>No bundle wallets created yet.</div>
                  )}
                </>
              )}

              <div style={{ marginTop: 18, fontSize: 15, fontWeight: 800, color: "#fff" }}>Recent Activity</div>
              {Array.isArray(toolDetails.tool.events) && toolDetails.tool.events.length ? (
                <div style={{ display: "flex", flexDirection: "column", gap: 8, marginTop: 10 }}>
                  {toolDetails.tool.events.slice(0, 10).map((event) => (
                    <div
                      key={event.id}
                      style={{
                        border: "1px solid rgba(255,255,255,.06)",
                        borderRadius: 12,
                        padding: 12,
                        background: "rgba(255,255,255,.02)",
                      }}
                    >
                      <div style={{ color: "#fff", fontSize: 12, lineHeight: 1.6 }}>{event.message}</div>
                      <div style={{ color: "rgba(255,255,255,.42)", fontSize: 11, marginTop: 6 }}>
                        {String(event.eventType || "").replace(/_/g, " ").toUpperCase()}
                        {event.tx ? ` • ${String(event.tx).slice(0, 8)}...${String(event.tx).slice(-6)}` : ""}
                        {event.createdAt ? ` • ${new Date(event.createdAt).toLocaleString()}` : ""}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ color: "rgba(255,255,255,.45)", fontSize: 12, marginTop: 10 }}>No tool activity yet.</div>
              )}
            </>
          )}
        </ToolModal>
      )}

      {fundingKeySecret && (
        <ToolModal title={`${fundingKeySecret.label || "Tool"} Funding Key`} onClose={() => setFundingKeySecret(null)}>
          <div style={{ color: "rgba(255,255,255,.7)", fontSize: 12, lineHeight: 1.7 }}>
            This private key controls the funding wallet for this tool instance. Anyone with this key can move the funds.
          </div>
          <div style={{ marginTop: 14, color: "rgba(255,255,255,.5)", fontSize: 11, fontWeight: 800, letterSpacing: 0.8 }}>
            WALLET
          </div>
          <div style={{ marginTop: 8, color: "#fff", fontSize: 12, wordBreak: "break-all" }}>
            <code>{fundingKeySecret.publicKey}</code>
          </div>
          <div style={{ marginTop: 14, color: "rgba(255,255,255,.5)", fontSize: 11, fontWeight: 800, letterSpacing: 0.8 }}>
            PRIVATE KEY
          </div>
          <textarea
            readOnly
            className="input-f"
            value={fundingKeySecret.secretKeyBase58 || ""}
            rows={4}
            style={{ marginTop: 8, resize: "vertical" }}
          />
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 14 }}>
            <button className="btn-fire" onClick={() => copyText(fundingKeySecret.publicKey, "Funding wallet")} style={{ padding: "10px 14px", fontSize: 12 }}>
              Copy Wallet
            </button>
            <button className="btn-ghost" onClick={() => copyText(fundingKeySecret.secretKeyBase58, "Funding key")} style={{ padding: "10px 14px", fontSize: 12 }}>
              Copy Private Key
            </button>
          </div>
        </ToolModal>
      )}

      {walletKeysSecret && (
        <ToolModal title={`${walletKeysSecret.label || "Tool"} Wallet Keys`} onClose={() => setWalletKeysSecret(null)}>
          <div style={{ color: "rgba(255,255,255,.7)", fontSize: 12, lineHeight: 1.7 }}>
            These private keys control the managed wallets attached to this tool. Anyone with these keys can move the funds.
          </div>
          <div style={{ display: "flex", flexDirection: "column", gap: 12, marginTop: 14 }}>
            {(Array.isArray(walletKeysSecret.wallets) ? walletKeysSecret.wallets : []).map((wallet) => (
              <div key={wallet.id || wallet.publicKey} style={{ border: "1px solid rgba(255,255,255,.06)", borderRadius: 12, padding: 12, background: "rgba(255,255,255,.02)" }}>
                <div style={{ color: "#fff", fontSize: 13, fontWeight: 800 }}>
                  {wallet.label || "Wallet"} {wallet.imported ? <span style={{ color: "rgba(255,214,184,.88)", fontSize: 11 }}>(Imported)</span> : null}
                </div>
                <div style={{ marginTop: 8, color: "rgba(255,255,255,.5)", fontSize: 11, fontWeight: 800, letterSpacing: 0.8 }}>
                  WALLET
                </div>
                <div style={{ marginTop: 6, color: "#fff", fontSize: 12, wordBreak: "break-all" }}>
                  <code>{wallet.publicKey}</code>
                </div>
                <div style={{ marginTop: 10, color: "rgba(255,255,255,.5)", fontSize: 11, fontWeight: 800, letterSpacing: 0.8 }}>
                  PRIVATE KEY
                </div>
                <textarea
                  readOnly
                  className="input-f"
                  value={wallet.secretKeyBase58 || ""}
                  rows={3}
                  style={{ marginTop: 8, resize: "vertical" }}
                />
                <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginTop: 10 }}>
                  <button className="btn-ghost" onClick={() => copyText(wallet.publicKey, `${wallet.label || "Wallet"} address`)} style={{ padding: "8px 12px", fontSize: 11 }}>
                    Copy Wallet
                  </button>
                  <button className="btn-fire" onClick={() => copyText(wallet.secretKeyBase58, `${wallet.label || "Wallet"} private key`)} style={{ padding: "8px 12px", fontSize: 11 }}>
                    Copy Private Key
                  </button>
                </div>
              </div>
            ))}
          </div>
        </ToolModal>
      )}

      {draftTool && (
        <ToolModal title={`Create ${draftTool.label}`} onClose={() => setDraftTool(null)}>
          <div style={{ color: "rgba(255,255,255,.62)", fontSize: 13, lineHeight: 1.7 }}>
            {draftTool.description}
          </div>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginTop: 16 }}>
            <div style={{ gridColumn: "1 / -1" }}>
              <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TITLE</label>
              <input className="input-f" value={draft.title} onChange={(e) => setDraft((prev) => ({ ...prev, title: e.target.value }))} />
            </div>
            {draftTool.targetKind === "mint" ? (
              <>
                {attachedMintOptions.length ? (
                  <div style={{ gridColumn: "1 / -1" }}>
                    <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>
                      ATTACHED TOKEN
                    </label>
                    <select
                      className="input-f"
                      value={attachedMintOptions.some((entry) => entry.mint === draft.targetMint) ? draft.targetMint : ""}
                      onChange={(e) => setDraft((prev) => ({ ...prev, targetMint: e.target.value }))}
                    >
                      <option value="">Choose from attached tokens</option>
                      {attachedMintOptions.map((token) => (
                        <option key={token.id || token.mint} value={token.mint}>
                          {token.symbol} • {token.mint}
                        </option>
                      ))}
                    </select>
                    <div style={{ color: "rgba(255,255,255,.45)", fontSize: 11, marginTop: 6 }}>
                      Pick one of your attached tokens or paste any mint below.
                    </div>
                  </div>
                ) : null}
                <div style={{ gridColumn: "1 / -1" }}>
                  <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>TOKEN MINT</label>
                  <input
                    className="input-f"
                    value={draft.targetMint}
                    onChange={(e) => setDraft((prev) => ({ ...prev, targetMint: e.target.value }))}
                    placeholder="Enter token CA / mint"
                  />
                </div>
              </>
            ) : (
              <div style={{ gridColumn: "1 / -1" }}>
                <label style={{ display: "block", fontSize: 10, fontWeight: 700, color: "rgba(255,255,255,.35)", marginBottom: 6 }}>DEXSCREENER URL</label>
                <input
                  className="input-f"
                  value={draft.targetUrl}
                  onChange={(e) => setDraft((prev) => ({ ...prev, targetUrl: e.target.value }))}
                  placeholder="https://dexscreener.com/..."
                />
              </div>
            )}
            <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", gap: 8 }}>
              <input
                id="tool-simple-mode"
                type="checkbox"
                checked={draft.simpleMode}
                onChange={(e) => setDraft((prev) => ({ ...prev, simpleMode: e.target.checked }))}
              />
              <label htmlFor="tool-simple-mode" style={{ color: "#fff", fontSize: 12, fontWeight: 700 }}>
                Start in simple mode
              </label>
            </div>
          </div>
          <div style={{ marginTop: 16, padding: "12px 14px", borderRadius: 14, border: "1px solid rgba(255,255,255,.06)", background: "rgba(255,255,255,.02)" }}>
            <div style={{ color: "#fff", fontSize: 12, fontWeight: 800 }}>Unlock Summary</div>
            <div style={{ color: "rgba(255,255,255,.58)", fontSize: 12, lineHeight: 1.7, marginTop: 6 }}>
              Unlock fee: {fmtSol(draftTool.unlockFeeSol)} SOL
              <br />
              Reserve: {fmtSol(draftTool.reserveSol)} SOL
              <br />
              Total funding required: {fmtSol(draftTool.requiredSol)} SOL
            </div>
          </div>
          {createError && <div style={{ color: "#ff9c9c", fontSize: 12, marginTop: 14 }}>{createError}</div>}
          <div style={{ display: "flex", gap: 10, marginTop: 16 }}>
            <button className="btn-fire" onClick={createInstance} disabled={createBusy} style={{ padding: "10px 16px", fontSize: 12 }}>
              {createBusy ? "Creating..." : `Create ${draftTool.label}`}
            </button>
            <button className="btn-ghost" onClick={() => setDraftTool(null)} style={{ padding: "10px 16px", fontSize: 12 }}>
              Cancel
            </button>
          </div>
        </ToolModal>
      )}
    </div>
  );
}


