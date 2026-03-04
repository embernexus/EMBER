export const fmt = (n) =>
  n >= 1e6 ? `${(n / 1e6).toFixed(2)}M` : n >= 1e3 ? `${(n / 1e3).toFixed(1)}K` : String(n ?? 0);

export const fmtFull = (n) => (n ?? 0).toLocaleString();

export const fmtSol = (n) => {
  const val = Number(n);
  return (Number.isFinite(val) ? val : 0).toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 6,
  });
};

export const fmtSec = (s) =>
  s < 60
    ? `${s}s`
    : s < 3600
      ? `${Math.floor(s / 60)}m ${s % 60}s`
      : `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;

export const fmtAge = (s) =>
  s === 0 ? "just now" : s < 60 ? `${s}s ago` : `${Math.floor(s / 60)}m ago`;

export const solscanTx = (sig) => `https://solscan.io/tx/${sig}`;
export const solscanAddr = (addr) => `https://solscan.io/account/${addr}`;
