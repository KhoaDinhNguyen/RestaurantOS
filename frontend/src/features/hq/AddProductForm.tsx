import { useState } from "react";
import { publishMenu } from "../../api/endpoints";
import type { MenuProductInput } from "../../api/types";
import { ApiStatus } from "../../components/ApiStatus";
import { Field, Panel } from "../../components/Panel";
import { buttonClass, inputClass } from "../../components/styles";
import { useAction } from "../../hooks/useAction";

const empty = {
  vendor_id: "",
  product_id: "",
  name: "",
  price_units: "",
  dish_id: "",
  // No default: a product's dish link is fixed at creation, so an edit must
  // not send a multiplier nobody typed (it would answer 409 dish_link_fixed).
  multiplier: "",
  note: "",
};

/**
 * `POST /menu/products`: add a product (no product id) or change one's name
 * or price (with its id). The gateway makes new ids; HQ answers 202 and the
 * region has the version once its `menu_version` reaches it.
 */
export function AddProductForm() {
  const [form, setForm] = useState(empty);
  const publish = useAction(publishMenu);
  const set = (key: keyof typeof empty) => (e: { target: { value: string } }) =>
    setForm({ ...form, [key]: e.target.value });

  const submit = () => {
    const product: MenuProductInput = {
      vendor_id: form.vendor_id.trim(),
      name: form.name,
      price_units: Number(form.price_units),
    };
    if (form.product_id.trim()) product.product_id = form.product_id.trim();
    if (form.dish_id.trim()) product.dish_id = form.dish_id.trim();
    if (form.multiplier.trim()) product.multiplier = Number(form.multiplier);
    void publish.run({ note: form.note || null, products: [product] });
  };

  const answer = publish.result;
  /** No product id: a new product, which needs its dish link. */
  const isNew = form.product_id.trim() === "";
  return (
    <Panel title="Add or change a product">
      <form
        className="flex flex-wrap items-end gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <Field label="vendor id">
          <input
            className={inputClass}
            value={form.vendor_id}
            onChange={set("vendor_id")}
            required
          />
        </Field>
        <Field label="product id (empty = new)">
          <input
            className={inputClass}
            value={form.product_id}
            onChange={set("product_id")}
          />
        </Field>
        <Field label="name">
          <input
            className={inputClass}
            value={form.name}
            onChange={set("name")}
            required
          />
        </Field>
        <Field label="price units">
          <input
            className={`${inputClass} w-24`}
            inputMode="numeric"
            value={form.price_units}
            onChange={set("price_units")}
            required
          />
        </Field>
        <Field label="dish id">
          <input
            className={inputClass}
            value={form.dish_id}
            onChange={set("dish_id")}
            required={isNew}
          />
        </Field>
        <Field label="multiplier">
          <input
            className={`${inputClass} w-16`}
            inputMode="numeric"
            value={form.multiplier}
            onChange={set("multiplier")}
            required={isNew}
          />
        </Field>
        <Field label="note">
          <input
            className={inputClass}
            value={form.note}
            onChange={set("note")}
          />
        </Field>
        <button
          className={buttonClass}
          type="submit"
          disabled={publish.pending}
        >
          Publish
        </button>
      </form>
      <p className="mt-2 text-xs text-slate-500">
        A new product needs a dish id and a multiplier; find a dish id with the
        dish search on the Customer view. On a change, leave both empty: a
        product&apos;s dish link cannot change.
      </p>
      {answer?.data && (
        <div className="mt-3 text-sm text-emerald-700">
          <p>
            {answer.status}: stored as menu version {answer.data.menu_version}.
          </p>
          <ul className="list-inside list-disc font-mono text-xs">
            {answer.data.products.map((p) => (
              <li key={`${p.vendor_id}|${p.product_id}`}>
                {p.vendor_id} / {p.product_id} in {p.region_id}:{" "}
                {p.created ? "created" : "changed or already there"}
              </li>
            ))}
          </ul>
        </div>
      )}
      <div className="mt-3">
        <ApiStatus result={answer} />
      </div>
    </Panel>
  );
}
