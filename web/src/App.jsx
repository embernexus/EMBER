import { useCallback, useEffect, useState } from "react";
import {
  apiAuthLogout,
  apiAuthMe,
  apiCreateToken,
  apiDeleteToken,
  apiDashboard,
  apiGenerateDepositAddress,
  apiVolumeSweep,
  apiVolumeWithdraw,
  apiVolumeWithdrawOptions,
  apiTokenLiveDetails,
  apiPublicMetrics,
  apiUpdateToken,
  isApiError,
} from "./api/client";
import { Embers, FireBg } from "./components/BackgroundFX";
import TokenTicker from "./components/dashboard/TokenTicker";
import Footer from "./components/layout/Footer";
import LanguageSwitcher from "./components/layout/LanguageSwitcher";
import NavBar from "./components/layout/NavBar";
import AttachModal from "./components/modals/AttachModal";
import DeployModal from "./components/modals/DeployModal";
import LoginModal from "./components/modals/LoginModal";
import { APP_CSS } from "./styles/appCss";
import BurnsPage from "./pages/BurnsPage";
import DashboardPage from "./pages/DashboardPage";
import DevLogsPage from "./pages/DevLogsPage";
import HomePage from "./pages/HomePage";
import ProtocolHubPage from "./pages/ProtocolHubPage";
import RoadmapPage from "./pages/RoadmapPage";
import WhitepaperPage from "./pages/WhitepaperPage";

const DEFAULT_PUBLIC_METRICS = {
  lifetimeIncinerated: 0,
  totalBotTransactions: 0,
  activeTokens: 0,
  totalHolders: 0,
  emberIncinerated: 0,
  totalRewardsProcessedSol: 0,
  totalFeesTakenSol: 0,
};

export default function App() {
  const [page, setPage] = useState("home");
  const [user, setUser] = useState(null);
  const [showLogin, setShowLogin] = useState(false);
  const [showAttach, setShowAttach] = useState(false);
  const [showDeploy, setShowDeploy] = useState(false);
  const [heroWord, setHeroWord] = useState("BURN");
  const [tokens, setTokens] = useState([]);
  const [feed, setFeed] = useState([]);
  const [allLogs, setAllLogs] = useState([]);
  const [chartData, setChartData] = useState([]);
  const [menuOpen, setMenuOpen] = useState(false);
  const [publicMetrics, setPublicMetrics] = useState(DEFAULT_PUBLIC_METRICS);

  const clearPrivateState = useCallback(() => {
    setTokens([]);
    setFeed([]);
    setAllLogs([]);
    setChartData([]);
  }, []);

  const loadDashboard = useCallback(async () => {
    const data = await apiDashboard();
    setTokens(Array.isArray(data.tokens) ? data.tokens : []);
    setFeed(Array.isArray(data.feed) ? data.feed : []);
    setAllLogs(Array.isArray(data.logs) ? data.logs : []);
    setChartData(Array.isArray(data.chartData) ? data.chartData : []);
    return data;
  }, []);

  const loadPublicMetrics = useCallback(async () => {
    try {
      const data = await apiPublicMetrics();
      setPublicMetrics(data);
    } catch {
      // keep defaults until API is available
    }
  }, []);

  useEffect(() => {
    loadPublicMetrics();
    const id = setInterval(loadPublicMetrics, 30_000);
    return () => clearInterval(id);
  }, [loadPublicMetrics]);

  useEffect(() => {
    const heroWords = ["BURN", "AUTO", "PUMP"];
    const id = setInterval(() => {
      setHeroWord((w) => {
        const idx = heroWords.indexOf(w);
        return heroWords[(idx + 1) % heroWords.length] || heroWords[0];
      });
    }, 2800);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      try {
        const data = await apiAuthMe();
        if (!mounted) return;
        if (data?.user?.username) {
          setUser(data.user.username);
          setPage("dashboard");
        } else {
          setUser(null);
          clearPrivateState();
        }
      } catch {
        if (!mounted) return;
        setUser(null);
        clearPrivateState();
      }
    };

    run();
    return () => {
      mounted = false;
    };
  }, [clearPrivateState]);

  useEffect(() => {
    if (!user) return;
    let active = true;

    const pull = async () => {
      try {
        await loadDashboard();
      } catch (error) {
        if (!active) return;
        const msg = String(error?.message || "").toLowerCase();
        const unauthorized = msg.includes("unauthorized") || (isApiError(error) && error.status === 401);
        if (unauthorized) {
          setUser(null);
          clearPrivateState();
          setPage("home");
        }
      }
    };

    pull();
    const id = setInterval(pull, 15_000);
    return () => {
      active = false;
      clearInterval(id);
    };
  }, [user, loadDashboard, clearPrivateState]);

  const handleSignOut = useCallback(async () => {
    try {
      await apiAuthLogout();
    } catch {
      // best-effort sign-out
    } finally {
      setUser(null);
      setMenuOpen(false);
      setShowAttach(false);
      setShowDeploy(false);
      setPage("home");
      clearPrivateState();
    }
  }, [clearPrivateState]);

  const handleAttachToken = useCallback(async (payload) => {
    const data = await apiCreateToken(payload);
    await loadDashboard();
    return data.token;
  }, [loadDashboard]);

  const handleTokenUpdate = useCallback(async (nextToken) => {
    const payload = {
      claimSec: Number(nextToken.claimSec),
      burnSec: Number(nextToken.burnSec),
      splits: Math.max(1, Math.floor(Number(nextToken.splits) || 1)),
      active: Boolean(nextToken.active),
      selectedBot: String(nextToken.selectedBot || nextToken.moduleType || "burn"),
      claimEnabled:
        nextToken.moduleConfig && typeof nextToken.moduleConfig.claimEnabled === "boolean"
          ? nextToken.moduleConfig.claimEnabled
          : true,
      tradeWalletCount:
        nextToken.moduleConfig && nextToken.moduleConfig.tradeWalletCount !== undefined
          ? Number(nextToken.moduleConfig.tradeWalletCount)
          : undefined,
      speed:
        nextToken.moduleConfig && nextToken.moduleConfig.speed !== undefined
          ? Number(nextToken.moduleConfig.speed)
          : undefined,
      aggression:
        nextToken.moduleConfig && nextToken.moduleConfig.aggression !== undefined
          ? Number(nextToken.moduleConfig.aggression)
          : undefined,
      minTradeSol:
        nextToken.moduleConfig && nextToken.moduleConfig.minTradeSol !== undefined
          ? Number(nextToken.moduleConfig.minTradeSol)
          : undefined,
      maxTradeSol:
        nextToken.moduleConfig && nextToken.moduleConfig.maxTradeSol !== undefined
          ? Number(nextToken.moduleConfig.maxTradeSol)
          : undefined,
    };
    const data = await apiUpdateToken(nextToken.id, payload);
    setTokens((prev) => prev.map((t) => (t.id === data.token.id ? data.token : t)));
    return data.token;
  }, []);

  const handleTokenDelete = useCallback(async (tokenId) => {
    await apiDeleteToken(tokenId);
    await loadDashboard();
    return true;
  }, [loadDashboard]);

  const handleVolumeSweep = useCallback(async (tokenId) => {
    const result = await apiVolumeSweep(tokenId);
    await loadDashboard();
    return result;
  }, [loadDashboard]);

  const handleVolumeWithdraw = useCallback(async (tokenId, payload) => {
    const result = await apiVolumeWithdraw(tokenId, payload || {});
    await loadDashboard();
    return result;
  }, [loadDashboard]);

  const onNavItemClick = useCallback((item) => {
    if (!item.enabled) return;

    if (item.key === "roadmap") {
      setMenuOpen(false);
      setPage("roadmap");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "burns") {
      setMenuOpen(false);
      setPage("burns");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "whitepaper") {
      setMenuOpen(false);
      setPage("whitepaper");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "docs") {
      setMenuOpen(false);
      setPage("docs");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "updates") {
      setMenuOpen(false);
      setPage("updates");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "dashboard") {
      if (!user) {
        setShowLogin(true);
        return;
      }
      setMenuOpen(false);
      setPage("dashboard");
      window.scrollTo({ top: 0, behavior: "smooth" });
      return;
    }
    if (item.key === "deploy") {
      setShowDeploy(true);
      return;
    }
    if (item.key === "how") {
      setMenuOpen(false);
      setPage("home");
      setTimeout(() => {
        document.getElementById("how-it-works")?.scrollIntoView({ behavior: "smooth" });
      }, 30);
    }
  }, [user]);

  const handleBrandClick = useCallback(() => {
    setMenuOpen(false);
    setPage("home");
  }, []);

  return (
    <>
      <style>{APP_CSS}</style>
      <FireBg />
      <Embers />

      <NavBar
        page={page}
        user={user}
        menuOpen={menuOpen}
        onToggleMenu={() => setMenuOpen((m) => !m)}
        onSignOut={handleSignOut}
        onShowLogin={() => setShowLogin(true)}
        onNavItemClick={onNavItemClick}
        publicMetrics={publicMetrics}
        onBrandClick={handleBrandClick}
      />

      <TokenTicker tokens={tokens} />
      <div style={{ height: 102 }} />

      {page === "home" && (
        <HomePage
          heroWord={heroWord}
          publicMetrics={publicMetrics}
          onShowLogin={() => {
            if (user) {
              setPage("dashboard");
              return;
            }
            setShowLogin(true);
          }}
        />
      )}

      {page === "roadmap" && <RoadmapPage />}
      {page === "burns" && (
        <BurnsPage
          tokens={tokens}
          allLogs={allLogs}
          publicMetrics={publicMetrics}
          chartData={chartData}
        />
      )}
      {page === "docs" && <ProtocolHubPage />}
      {page === "whitepaper" && <WhitepaperPage />}
      {page === "updates" && <DevLogsPage />}

      {page === "dashboard" && user && (
        <DashboardPage
          user={user}
          tokens={tokens}
          allLogs={allLogs}
          chartData={chartData}
          feed={feed}
          onShowAttach={() => setShowAttach(true)}
          onUpdateToken={handleTokenUpdate}
          onDeleteToken={handleTokenDelete}
          onFetchTokenDetails={apiTokenLiveDetails}
          onFetchVolumeWithdrawOptions={apiVolumeWithdrawOptions}
          onVolumeSweep={handleVolumeSweep}
          onVolumeWithdraw={handleVolumeWithdraw}
        />
      )}

      <Footer />
      <LanguageSwitcher />

      {showLogin && (
        <LoginModal
          onClose={() => setShowLogin(false)}
          onLogin={async (username) => {
            setUser(username);
            setShowLogin(false);
            setShowDeploy(false);
            setPage("dashboard");
            try {
              await loadDashboard();
            } catch {
              // login succeeds even if dashboard fetch races API startup
            }
          }}
        />
      )}

      {showAttach && (
        <AttachModal
          onClose={() => setShowAttach(false)}
          onAttach={handleAttachToken}
          onGenerateDeposit={apiGenerateDepositAddress}
        />
      )}

      {showDeploy && (
        <DeployModal
          onClose={() => setShowDeploy(false)}
          user={user}
          onRequireLogin={() => setShowLogin(true)}
          onGoDashboard={() => {
            setShowDeploy(false);
            setPage("dashboard");
          }}
          onAutoAttach={async () => {
            await loadDashboard();
          }}
        />
      )}
    </>
  );
}
