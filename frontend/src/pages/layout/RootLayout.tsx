import { useState } from "react";
import { NavLink, Outlet, useLocation } from "react-router";
import { ClockBar } from "../../features/clock/ClockBar";
import { SimClockProvider } from "../../features/clock/SimClockProvider";
import { CustomerPage } from "../CustomerPage";
import { HqPage } from "../HqPage";
import { OpsPage } from "../OpsPage";
import { RegionPage } from "../RegionPage";
import { ViewActiveContext } from "./viewActiveContext";

const VIEWS = [
  { to: "/hq", label: "HQ", Page: HqPage },
  { to: "/region", label: "Region", Page: RegionPage },
  { to: "/customer", label: "Customer", Page: CustomerPage },
  { to: "/ops", label: "Ops", Page: OpsPage },
];

/**
 * Header with the four views and the simulated clock; the views below.
 *
 * All four views stay mounted and only the current one is shown, so each
 * keeps what you typed, picked and loaded while you look at another one. A
 * hidden view stops polling (`ViewActiveContext`). Reloading the page starts
 * every view afresh.
 */
export function RootLayout() {
  const location = useLocation();
  const path = location.pathname.replace(/\/+$/, "") || "/";
  const current = VIEWS.find((v) => v.to === path);
  // The address each view was last left at (e.g. /region?region=r2), so its
  // tab brings it back.
  const [lastSearch, setLastSearch] = useState<Record<string, string>>({});

  return (
    <SimClockProvider>
      <div className="min-h-screen">
        <header className="bg-slate-900 text-white">
          <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-3 px-4 py-3">
            <div className="flex items-center gap-6">
              <span className="font-semibold">RestaurantOS</span>
              {import.meta.env.MODE === "mock" && (
                <span className="rounded bg-amber-400 px-1.5 text-xs font-semibold text-slate-900">
                  MOCK DATA
                </span>
              )}
              <nav className="flex gap-1">
                {VIEWS.map((v) => (
                  <NavLink
                    key={v.to}
                    to={
                      v.to +
                      (v === current
                        ? location.search
                        : (lastSearch[v.to] ?? ""))
                    }
                    onClick={() => {
                      if (current)
                        setLastSearch((s) => ({
                          ...s,
                          [current.to]: location.search,
                        }));
                    }}
                    className={({ isActive }) =>
                      `rounded px-3 py-1 text-sm ${
                        isActive
                          ? "bg-white text-slate-900"
                          : "hover:bg-slate-700"
                      }`
                    }
                  >
                    {v.label}
                  </NavLink>
                ))}
              </nav>
            </div>
            <ClockBar />
          </div>
        </header>
        <main className="mx-auto max-w-7xl px-4 py-6">
          {VIEWS.map(({ to, Page }) => (
            <div key={to} hidden={current?.to !== to}>
              <ViewActiveContext.Provider value={current?.to === to}>
                <Page />
              </ViewActiveContext.Provider>
            </div>
          ))}
          {!current && <Outlet />}
        </main>
      </div>
    </SimClockProvider>
  );
}
