import { ClusterPanel } from "../features/cluster/ClusterPanel";
import { AddProductForm } from "../features/hq/AddProductForm";
import { EventsPanel, ExceptionsPanel } from "../features/hq/EventsPanel";
import { IngredientUsagePanel } from "../features/hq/IngredientUsagePanel";
import { TopDishesPanel } from "../features/hq/TopDishesPanel";
import { PageLayout } from "./layout/PageLayout";

export function HqPage() {
  return (
    <PageLayout
      title="Head office"
      subtitle="City-wide queries with their meta, cluster health, manual failover and menu changes."
    >
      <ClusterPanel canFailover />
      <div className="grid gap-4 lg:grid-cols-2">
        <TopDishesPanel />
        <IngredientUsagePanel />
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <EventsPanel />
        <ExceptionsPanel />
      </div>
      <AddProductForm />
    </PageLayout>
  );
}
