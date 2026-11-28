import { useState } from "react";
import { Field } from "../components/Panel";
import { inputClass } from "../components/styles";
import { DishNearbySearch } from "../features/customer/DishNearbySearch";
import { OrderForm, type PlacedOrder } from "../features/customer/OrderForm";
import { OrderTracker } from "../features/customer/OrderTracker";
import { VendorSearch } from "../features/customer/VendorSearch";
import { PageLayout } from "./layout/PageLayout";

/** A delivery-app user: find a restaurant or a dish, order, follow the order. */
export function CustomerPage() {
  const [geohash, setGeohash] = useState("w21z7");
  const [vendorId, setVendorId] = useState<string | null>(null);
  const [placed, setPlaced] = useState<PlacedOrder | null>(null);

  return (
    <PageLayout
      title="Customer"
      subtitle="Search, order and follow the order. Every order goes through the gateway."
      toolbar={
        <Field label="my geohash cell">
          <input
            className={`${inputClass} w-28 font-mono`}
            value={geohash}
            onChange={(e) => setGeohash(e.target.value.trim())}
          />
        </Field>
      }
    >
      <div className="grid gap-4 lg:grid-cols-2">
        <VendorSearch
          geohash={geohash}
          selected={vendorId}
          onSelect={(id) => {
            setVendorId(id);
          }}
        />
        <DishNearbySearch geohash={geohash} />
      </div>
      {vendorId && (
        <OrderForm
          key={vendorId}
          vendorId={vendorId}
          geohash={geohash}
          onPlaced={setPlaced}
        />
      )}
      {placed && <OrderTracker key={placed.orderId} order={placed} />}
    </PageLayout>
  );
}
