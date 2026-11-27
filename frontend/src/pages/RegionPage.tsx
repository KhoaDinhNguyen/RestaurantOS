import { Fragment, useState } from "react";
import { useSearchParams } from "react-router";
import { REGION_IDS } from "../api/endpoints";
import { Field } from "../components/Panel";
import { buttonClass, inputClass } from "../components/styles";
import { KitchenBoard } from "../features/region/KitchenBoard";
import { MenuAvailabilityPanel } from "../features/region/MenuAvailabilityPanel";
import { RegionSummaryPanel } from "../features/region/RegionSummaryPanel";
import { StockPanel } from "../features/region/StockPanel";
import { useSimClock } from "../hooks/useSimClock";
import { useViewActive } from "../hooks/useViewActive";
import { PageLayout } from "./layout/PageLayout";

/**
 * The regional office and its kitchens. Everything here goes to the region's
 * own edge API (`/edge/{region}/...`), so it keeps working while the region
 * is cut off from HQ.
 */
export function RegionPage() {
  const { day } = useSimClock();
  const [params, setParams] = useSearchParams();
  const active = useViewActive();
  // The view stays mounted while another tab is open, and the address then
  // belongs to that tab. So the region and vendor live in state, which
  // follows the address while this view is on screen (a bookmarked
  // /region?region=r2&vendor=... works, and so do Back and Forward), and
  // keeps its values while another view is.
  const fromUrl = {
    regionId: params.get("region") ?? REGION_IDS[0] ?? "r1",
    vendorId: params.get("vendor"),
  };
  const [shown, setShown] = useState(fromUrl);
  const [typedVendor, setTypedVendor] = useState(fromUrl.vendorId ?? "");
  if (
    active &&
    (fromUrl.regionId !== shown.regionId || fromUrl.vendorId !== shown.vendorId)
  ) {
    if (fromUrl.vendorId !== shown.vendorId)
      setTypedVendor(fromUrl.vendorId ?? "");
    setShown(fromUrl);
  }
  const { regionId, vendorId } = shown;

  // Only used while this view is on screen, so the address is enough: the
  // state above follows it.
  const show = (region: string, vendor: string | null) =>
    setParams(vendor ? { region, vendor } : { region });

  return (
    <PageLayout
      title="Region"
      subtitle="Regional office and kitchens, served by the region's edge API."
      toolbar={
        <div className="flex flex-wrap items-end gap-2">
          <Field label="region">
            <select
              className={inputClass}
              value={regionId}
              onChange={(e) => show(e.target.value, null)}
            >
              {REGION_IDS.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </Field>
          <form
            className="flex items-end gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              show(regionId, typedVendor.trim() || null);
            }}
          >
            <Field label="restaurant (vendor id)">
              <input
                className={inputClass}
                value={typedVendor}
                onChange={(e) => setTypedVendor(e.target.value)}
              />
            </Field>
            <button className={buttonClass} type="submit">
              Open
            </button>
          </form>
        </div>
      }
    >
      <RegionSummaryPanel regionId={regionId} day={day} />
      {vendorId ? (
        // Keyed by region and vendor: opening another restaurant starts its
        // panels afresh, so no Retry or message of the previous one survives.
        <Fragment key={`${regionId}|${vendorId}`}>
          <KitchenBoard regionId={regionId} vendorId={vendorId} day={day} />
          <div className="grid gap-4 lg:grid-cols-2">
            <MenuAvailabilityPanel regionId={regionId} vendorId={vendorId} />
            <StockPanel regionId={regionId} vendorId={vendorId} />
          </div>
        </Fragment>
      ) : (
        <p className="text-sm text-slate-500">
          Enter a vendor id to open its kitchen board, menu and stock.
        </p>
      )}
    </PageLayout>
  );
}
