import { ClusterPanel } from "../features/cluster/ClusterPanel";
import { FaultsPanel } from "../features/ops/FaultsPanel";
import { NodesPanel } from "../features/ops/NodesPanel";
import { PlaybackPanel } from "../features/ops/PlaybackPanel";
import { PageLayout } from "./layout/PageLayout";

/** The operator, outside the system: playback, faults, nodes. */
export function OpsPage() {
  return (
    <PageLayout
      title="Ops"
      subtitle="Outside the system: drive playback, break links, kill and start nodes."
    >
      <PlaybackPanel />
      <ClusterPanel />
      <div className="grid gap-4 lg:grid-cols-2">
        <FaultsPanel />
        <NodesPanel />
      </div>
    </PageLayout>
  );
}
