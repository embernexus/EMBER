export const KNOWN_MINTS = {
  DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263: { symbol: "BONK", name: "Bonk", pictureUrl: "https://cryptologos.cc/logos/bonk-bonk-logo.png" },
  EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm: { symbol: "WIF", name: "dogwifhat", pictureUrl: "https://cryptologos.cc/logos/dogwifcoin-wif-logo.png" },
  HhJpBhRRn4g85VZfzpmAn1CzCGCp4MQzR7Sn5dLNbwN: { symbol: "MYRO", name: "Myro", pictureUrl: "https://cryptologos.cc/logos/myro-myro-logo.png" },
};

export function resolveMint(mint) {
  const value = String(mint || "").trim();
  if (!value || value.length < 32) return null;
  if (KNOWN_MINTS[value]) return KNOWN_MINTS[value];
  return {
    symbol: value.slice(0, 4).toUpperCase(),
    name: `Token (${value.slice(0, 6)}...)`,
    pictureUrl: "",
  };
}

export function fmtInt(n) {
  return Number(n || 0).toLocaleString("en-US");
}

export function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function fakeTx() {
  const chars = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
  let output = "";
  for (let i = 0; i < 36; i += 1) {
    output += chars[Math.floor(Math.random() * chars.length)];
  }
  return output;
}
