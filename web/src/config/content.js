export const HOW_IT_WORKS_STEPS = [
  {
    n: "01",
    icon: "\u{1FA99}",
    title: "Reserve A Branded Wallet",
    body: "When you attach or deploy through EMBER, the platform reserves a prebuilt EMBR or EMBER vanity wallet from the pool. That wallet is assigned exclusively to your token so on-chain activity is easy to identify.",
  },
  {
    n: "02",
    icon: "\u{1F4CD}",
    title: "Deploy Or Attach",
    body: "Deploy from a connected wallet or launch directly from a reserved EMBR/EMBER wallet. Deploying from the branded wallet makes that address the real creator wallet on-chain, while attached bots still run as execution wallets rather than the coin creator.",
  },
  {
    n: "03",
    icon: "\u26A1",
    title: "Choose Funding Mode",
    body: "Run automation from creator rewards, external SOL deposits, or both. If creator rewards are enabled, set your pump.fun destination to your EMBER execution address. External funding is always available by sending SOL directly to that same address.",
  },
  {
    n: "04",
    icon: "\u{1F4B8}",
    title: "Funding Intake + Auto Processing",
    body: "On your configured interval (minimum 60 seconds), EMBER processes whichever funding inputs are enabled for that module: creator-reward claims, external wallet deposits, or both. Standard fee routing is 10% total (5% treasury / 5% $EMBER buyback + burn). Referred accounts route 5% to the referrer and OG accounts run fee-free.",
  },
  {
    n: "05",
    icon: "\u{1F504}",
    title: "Strategy Executes",
    body: "Available balance is executed according to your bot configuration. Burn Bot routes buybacks into supply reduction, Volume Bot drives controlled chart activity, Market Maker Bot runs attached-token two-sided execution around a target inventory posture, DCA Bot steadily accumulates, and Rekindle Bot buys real pullbacks on the attached chart.",
  },
  {
    n: "06",
    icon: "\u{1F525}",
    title: "Supply Reduction (When Enabled)",
    body: "If Burn Bot is enabled, bought tokens are sent directly to the Solana incinerator - a program-controlled destination that permanently removes tokens from circulation.",
  },
  {
    n: "07",
    icon: "\u{1F4CA}",
    title: "Track Everything On-Chain",
    body: "Every claim, external transfer, swap, and execution action is logged with a real transaction signature you can verify on Solscan. Your dashboard updates in real time with source-aware events so you always know exactly what happened and why.",
  },
  {
    n: "08",
    icon: "\u{1F465}",
    title: "Share Access Carefully",
    body: "Primary accounts can create one secondary manager login for shared operations. Managers can run bots and edit settings, but withdraw, sweep, delete, and account-level payout actions remain locked to the primary account.",
  },
];

export const ROADMAP_PHASES = [
  {
    phase: "Phase 1 - Foundation",
    status: "Completed",
    tone: "rgba(120,255,170,.16)",
    border: "rgba(120,255,170,.45)",
    text: "#9bffbf",
    items: [
      "EMBER token launch on Solana",
      "Core EMBER burn bot architecture and routing",
      "Token attach flow and burn tracking dashboard",
      "Creator fee recycling engine",
      "In-app deploy flow (image upload + metadata + PumpPortal launch)",
      "Wallet-key encryption and per-account bot isolation",
      "Production database setup with backup/export runbook",
      "Live logs and on-chain verification baseline",
    ],
  },
  {
    phase: "Phase 2 - Growth Engine",
    status: "In Progress",
    tone: "rgba(255,170,60,.14)",
    border: "rgba(255,170,60,.4)",
    text: "#ffbc74",
    items: [
      "Volume bot v1 (creator rewards, external funding, hybrid funding)",
      "Market maker bot (inventory-aware two-sided execution with migration-aware routing)",
      "DCA bot for recurring attached-token accumulation",
      "Rekindle bot for pullback-triggered attached-token dip buying",
      "Manager access (shared secondary login with restricted custody actions)",
      "Telegram user alerts with smart instant/digest delivery",
      "OG account tier with 0% protocol fees",
      "Referral earnings ledger with in-dashboard claiming to owned deposit wallets",
      "Other platform integrations",
      "Execution reliability hardening and retry logic",
      "Ops monitoring and alerting for live bot health",
      "Performance and uptime targets for production runtime",
    ],
  },
  {
    phase: "Phase 3 - Expansion",
    status: "Upcoming",
    tone: "rgba(140,180,255,.14)",
    border: "rgba(140,180,255,.35)",
    text: "#bcd3ff",
    items: [
      "Browser extension v1 (deploy, attach, start/stop, live logs)",
      "Dapp store submission and distribution",
      "Expanded shared access beyond single-manager mode",
      "Public API keys and webhook events for integrations",
      "Cross-chain expansion research",
      "Advanced analytics and execution controls",
    ],
  },
  {
    phase: "Phase 4 - Ecosystem",
    status: "Future",
    tone: "rgba(190,140,255,.14)",
    border: "rgba(190,140,255,.35)",
    text: "#d4b8ff",
    items: [
      "Exchange-readiness program (ops, compliance, infrastructure)",
      "Native Android APK release",
      "Native iOS app release",
      "Cross-chain execution modules after Solana stack maturity",
      "Partner integrations and ecosystem tooling",
    ],
  },
];

export const DEV_LOGS = [
  {
    version: "v1.5.0",
    date: "March 6, 2026",
    channel: "Production",
    title: "Billing + Referrals + Vanity Wallet Expansion",
    summary: "OG accounts, referral earnings, branded wallet flows, and updated protocol fee routing are now live.",
    changes: [
      "Standard protocol fee moved to 10% with treasury/burn routing.",
      "OG accounts now run with 0% protocol fees and visible account badging.",
      "Referral balances accrue on referred fee flow and can be claimed to owned deposit wallets.",
      "EMBR/EMBER wallet-pool flow is now surfaced more clearly in deploy and account operations.",
      "Protocol-owned bot runtime can now run beyond burn-only mode.",
    ],
  },
  {
    version: "v1.4.0",
    date: "March 6, 2026",
    channel: "Production",
    title: "Market Maker + Manager + New Strategy Bots",
    summary: "Attached-token market making, restricted shared account access, DCA, and Rekindle are now live.",
    changes: [
      "Market Maker Bot added with inventory targeting and migration-aware routing.",
      "DCA Bot added for recurring attached-token accumulation from rewards, deposits, or both.",
      "Rekindle Bot added for pullback-triggered attached-token buying with cooldown-aware dip handling.",
      "Primary accounts can provision one manager login with restricted custody permissions.",
      "Dashboard controls now enforce manager-safe restrictions for withdraw, sweep, and delete.",
    ],
  },
  {
    version: "v1.0.0",
    date: "March 4, 2026",
    channel: "Production",
    title: "Token Launch Baseline",
    summary: "Core protocol launch baseline is now live.",
    changes: [
      "EMBER token launch completed.",
      "Burn bot execution baseline activated.",
      "On-chain logging and dashboard tracking enabled.",
    ],
  },
  {
    version: "v1.0.1",
    date: "March 3, 2026",
    channel: "Production",
    title: "Burn Bot Runtime Update",
    summary: "Burn cycle execution was stabilized for live operation.",
    changes: [
      "Burn bot scheduling hardened.",
      "Execution and transaction handling improved.",
      "Failure handling paths tightened.",
    ],
  },
  {
    version: "v1.1.0",
    date: "March 2, 2026",
    channel: "Production",
    title: "Volume Bot Added",
    summary: "Volume module foundation was added to the protocol stack.",
    changes: [
      "Volume bot config and runtime path added.",
      "Wallet fan-out and sweep flow added.",
      "Volume transaction tracking enabled.",
    ],
  },
  {
    version: "v1.2.0",
    date: "March 1, 2026",
    channel: "Production",
    title: "Platform Expansion Prep",
    summary: "Cross-platform rollout preparation started.",
    changes: [
      "Other platform integration planning started.",
      "Dapp store and extension scope defined.",
      "Mobile release path (Android/iOS) scoped.",
    ],
  },
];

export const DEV_QUEUE = [
  "Wire production auth session flow end-to-end in modal actions.",
  "Hook burn analytics to persisted on-chain event ingestion only.",
  "Add per-module fee schedule blocks once bot pricing is finalized.",
  "Publish token contract and replace placeholder contract references.",
];

export const DOC_SECTIONS = [
  {
    id: "start",
    title: "Getting Started",
    text: "Deploy your token, attach automation modules, and run each bot from creator rewards, external deposits, or both from one surface.",
    points: [
      "Create account and sign in to unlock Nexus dashboard.",
      "Deploy directly from EMBER or attach an existing Solana token by mint.",
      "Wallets are reserved from a branded EMBR/EMBER pool so deposit and execution flow is easy to recognize on-chain.",
      "Choose module strategy and funding mode per bot, then monitor all on-chain events in real time.",
      "Primary accounts can optionally add one manager login for shared day-to-day operations without handing over custody actions.",
      "Referral codes can be applied at signup so referred accounts share fee flow back to the referrer.",
      "Primary owners can claim referral earnings to any deposit wallet on their account.",
    ],
  },
  {
    id: "deploy",
    title: "Deploy Flow",
    text: "EMBER deploys through PumpPortal with your metadata and returns direct launch links, with an optional branded-wallet launch path.",
    points: [
      "Upload token media, optional launch banner, set name/symbol/description, and submit deploy.",
      "Enter either SOL amount or token amount for initial buy; both fields stay synchronized.",
      "You can deploy from a reserved EMBR/EMBER wallet instead of connecting a wallet directly.",
      "The branded deploy path shows the exact wallet to fund, required launch balance, and the wallet private key with explicit custody warnings.",
      "If you deploy from the reserved branded wallet, that wallet is the actual creator wallet seen on-chain.",
      "Deploy response returns mint, transaction signature, and Pump.fun coin link.",
      "Optional auto-attach adds a bot to Nexus in paused mode so you can configure and start manually.",
    ],
  },
  {
    id: "modules",
    title: "Execution Modules",
    text: "EMBER supports stacked execution logic per token with configurable timing, pacing, and funding controls.",
    points: [
      "Each module can run on creator rewards only, external funding only, or hybrid mode.",
      "Creator reward claiming can be enabled/disabled per module where supported; if disabled, the module executes only from external deposits.",
      "Shared/designated creator rewards can take a few minutes to become claimable; temporary 0.00 claimable states can occur before balances update.",
      "Protocol-owned creator rewards are allocated 50% to treasury and 50% to EMBER buyback + burn.",
      "Standard user fees route 10% total as 5% treasury and 5% EMBER buyback + burn.",
      "Referred accounts keep the same 10% total fee, but 5% accrues to the referrer while treasury and burn receive 2.5% each.",
      "OG accounts run fee-free.",
      "Burn Bot: reward-to-buyback-to-incineration automation.",
      "Volume Bot: controlled market activity for stronger visibility.",
      "Market Maker Bot: attached-token two-sided execution with target inventory control and migration-aware routing.",
      "DCA Bot: recurring attached-token accumulation with auto-sized recurring buys.",
      "Rekindle Bot: pullback-triggered attached-token buying with cooldown-aware dip detection.",
      "AI Trading Bot: strategy execution within configured constraints.",
    ],
  },
  {
    id: "access",
    title: "Account Access Model",
    text: "EMBER currently supports a primary account plus one optional manager login. This keeps shared operations simple without introducing a full workspace system yet.",
    points: [
      "Primary account retains full control over setup, bot execution, wallet access, and account-level management.",
      "Manager login can start/stop bots, edit configs, and work from the same dashboard as the owner.",
      "Manager login cannot withdraw funds, sweep balances, delete tokens, or manage access settings.",
      "This model is designed for token teams that need shared daily operations without handing over custody permissions.",
    ],
  },
  {
    id: "ops",
    title: "Ops + Monitoring",
    text: "Every critical action is logged for transparency and operational confidence.",
    points: [
      "Event feed tracks deploy, external funding, creator-reward claims, buybacks, burns, and failures.",
      "Funding-source attribution is preserved in logs so each execution can be traced back to rewards or external deposits.",
      "Referral earnings are tracked in-account with claimable balances and recent fee events.",
      "Direct Telegram alerts can be connected per login with smart instant/digest delivery controls.",
      "Burn tracker provides charting, token breakdown, and transaction history.",
      "Public metrics endpoint surfaces core protocol telemetry.",
    ],
  },
  {
    id: "security",
    title: "Security Model",
    text: "Protocol design prioritizes account isolation, encrypted key custody, and verifiable execution trails.",
    points: [
      "Session-based authentication and per-account token ownership checks.",
      "Primary/manager separation keeps custody actions limited to the owner account.",
      "Bot/deposit wallets are generated as vanity addresses with EMBR/EMBER prefixes for transparent attribution and branding.",
      "Private keys are encrypted at rest with AES-256-GCM and decrypted only inside backend execution workers for signing.",
      "Per-account wallet ownership is enforced in Postgres so user bot state never mixes between accounts.",
      "Config validation before execution to reduce operator mistakes.",
      "Solscan-linked transaction references for independent verification.",
    ],
  },
  {
    id: "wallets",
    title: "EMBR/EMBER Wallet Model",
    text: "Every execution module uses protocol-generated EMBR/EMBER wallets to keep on-chain attribution consistent, transparent, and brand aligned.",
    points: [
      "Deposit, deploy, and module trade wallets are generated with Solana vanity-prefix targeting (EMBR or EMBER).",
      "Address pools are pre-warmed so new token setup can reserve a branded wallet quickly instead of waiting for a fresh grind every time.",
      "Secret keys are never sent to the frontend and are not exposed in API responses.",
      "The only exception is the explicit branded deploy flow, where the deploy-wallet private key is shown once to the owner because that wallet is the actual creator wallet.",
      "Execution workers decrypt keys server-side only at signing time, then submit transactions to Solana RPC.",
      "This model keeps bot operations attributable on-chain while preserving key custody controls.",
    ],
  },
];

export const DOC_QUICK_LINKS = [
  { label: "Public Metrics API", value: "GET /api/public-metrics" },
  { label: "Dashboard API", value: "GET /api/dashboard" },
  { label: "Deploy Record API", value: "POST /api/deploy/record" },
  { label: "On-Chain Deploy", value: "POST https://pumpportal.fun/api/trade-local" },
  { label: "Attach Token API", value: "POST /api/tokens" },
  { label: "Update Token API", value: "PATCH /api/tokens/:id" },
];

export const DOC_FAQ = [
  {
    q: "What does deploy return after launch?",
    a: "Deploy returns the mint address, transaction reference, and a direct Pump.fun coin link for the newly launched token.",
  },
  {
    q: "Can one account run multiple token automations?",
    a: "Yes. Multiple token configs can run under one account, each with its own schedules and module settings.",
  },
  {
    q: "Can I share dashboard access with someone else?",
    a: "Yes. The primary account can create one manager login. Managers can operate bots and change configs, but cannot withdraw funds, sweep balances, or delete tokens.",
  },
  {
    q: "Where can users verify bot activity?",
    a: "On the burn tracker and transaction links to Solscan, where each logged event can be independently validated.",
  },
  {
    q: "How are protocol fees described currently?",
    a: "Standard protocol fee is 10% total (5% treasury / 5% EMBER buyback + burn). Referred accounts keep the same 10% total but route 5% to the referrer and 2.5%/2.5% to treasury/burn. OG accounts pay 0%. Protocol-owned creator rewards are still split 50/50 between treasury and EMBER buyback + burn.",
  },
  {
    q: "What is the difference between an attached bot wallet and a branded deploy wallet?",
    a: "An attached bot wallet is an EMBR/EMBER execution wallet used for automation. A branded deploy wallet is the actual launch wallet for the token, so its on-chain buys, sells, and creator state are treated as the real creator wallet activity.",
  },
  {
    q: "Can bots run from external funding only?",
    a: "Yes. For supported modules, disable creator-reward claiming and fund the bot wallet directly with SOL. The bot will execute using external funding only.",
  },
  {
    q: "Why does claimable creator rewards sometimes show 0.00?",
    a: "For shared/designated creator rewards, claimable balances are not always instant. It can take a few minutes before rewards appear as claimable.",
  },
  {
    q: "Why do bot wallets start with EMBR/EMBER?",
    a: "Branded vanity prefixes make bot and deposit wallets easy to identify on-chain for transparency, while private keys remain encrypted server-side for security.",
  },
];

export const WHITEPAPER_SECTIONS = [
  {
    id: "abstract",
    title: "1. Executive Overview",
    paragraphs: [
      "EMBER is a Solana automation protocol operating a multi-bot execution network for token projects.",
      "The platform runs advanced execution bots across burn automation, volume orchestration, market-making, AI trading, and specialized strategy modules with on-chain transparency.",
    ],
  },
  {
    id: "problem",
    title: "2. Problem Statement",
    paragraphs: [
      "Most token teams cannot run institutional-grade execution across burn, liquidity, and volume operations at scale.",
      "Manual workflows create fragmented execution, inconsistent outcomes, weak transparency for communities, and poor access control when multiple people need to operate the same token.",
    ],
    bullets: [
      "Running many bot types with consistent controls is operationally complex.",
      "Volume, liquidity, and burn actions require coordinated scheduling and risk controls.",
      "Communities need transaction-level proof across every bot action in real time.",
    ],
  },
  {
    id: "architecture",
    title: "3. Protocol Architecture",
    paragraphs: [
      "EMBER uses a three-layer model: web control panel, API service, and worker execution loop. Each layer has clear responsibilities.",
    ],
    bullets: [
      "Frontend: user auth, token setup, module controls, and execution visibility.",
      "API: account isolation, token config, deploy orchestration, funding-mode policy validation, and event data access.",
      "Worker cluster: burn bots, volume bots, market-maker bots, AI trading bots, and pluggable strategy modules.",
      "Account model: one primary owner plus one optional restricted manager login.",
      "Database: Postgres source of truth for token state and history.",
      "Deploy path: PumpPortal-backed launch pipeline with metadata upload, mint/tx response, and Pump.fun link output.",
    ],
  },
  {
    id: "engine",
    title: "4. Execution Bot Lifecycle",
    paragraphs: [
      "Each configured token can run multiple execution modules concurrently under deterministic schedules, policy constraints, and flexible funding inputs.",
    ],
    bullets: [
      "Token can be deployed from EMBER using uploaded metadata and synchronized SOL/token initial-buy inputs.",
      "Zero initial buy-in is permitted and explicitly warned before deploy submission.",
      "Token is attached with mint and protocol-generated deposit address.",
      "If deploy auto-attach is enabled, the bot is created paused and requires manual configure/start in Nexus.",
      "Creator rewards, external funding, or both can be routed into the configured execution pipeline.",
      "External deposits are accepted directly to the module deposit wallet and become execution-ready after policy checks.",
      "When creator claiming is disabled for a module, execution runs external-only and skips claim polling.",
      "Bots execute actions such as buy, sell, inventory rebalancing, liquidity support, and burn routing.",
      "AI trading modules can apply strategy logic within configured execution limits.",
      "Every action is source-tagged, signed, logged, and exposed with a verifiable transaction reference.",
    ],
  },
  {
    id: "fees",
    title: "5. Fee Model",
    paragraphs: [
      "Protocol-owned creator rewards are split 50% to treasury and 50% to EMBER buyback + burn.",
    ],
    bullets: [
      "50% Treasury allocation for infrastructure, uptime, operations, and maintenance.",
      "50% allocated to EMBER buyback and burn flow.",
      "Current claim-execution fee is 5% per claim: 2.5% treasury and 2.5% EMBER buyback + burn.",
    ],
    paragraphs2: [
      "These treasury and buyback allocations keep execution infrastructure reliable while adding ongoing burn pressure to the EMBER ecosystem.",
      "Fee schedules for volume bots, market-maker bots, AI trading bots, and other future modules evolve as each module reaches production maturity.",
    ],
  },
  {
    id: "token",
    title: "6. EMBER Token Framework",
    paragraphs: [
      "The EMBER token layer aligns protocol usage, execution demand, and long-term deflationary mechanics across the full bot stack.",
    ],
    bullets: [
      { kind: "contract" },
      { kind: "creator" },
      { kind: "treasury" },
      { kind: "incinerator" },
    ],
  },
  {
    id: "security",
    title: "7. Security and Controls",
    paragraphs: [
      "EMBER prioritizes operational safety, account isolation, and audit-friendly data exposure.",
    ],
    bullets: [
      "Username/password auth with server-side session cookies.",
      "Per-account token limits and ownership isolation.",
      "Restricted manager access for shared operations without custody rights.",
      "Policy-based execution controls per bot module.",
      "Bot/deposit wallets are generated as EMBR/EMBER vanity addresses for transparent on-chain attribution and branding.",
      "Wallet private keys are encrypted at rest (AES-256-GCM) and decrypted only inside execution workers when signatures are required.",
      "Signed transaction references exposed for independent verification.",
      "Incremental rollout plan before full production scaling.",
    ],
  },
  {
    id: "disclosure",
    title: "8. Disclosures",
    paragraphs: [
      "EMBER provides automation infrastructure and transparency tooling. It is not investment advice.",
      "Protocol parameters may evolve as reliability, market structure, and launch requirements are finalized.",
    ],
  },
];
