import { Link } from "react-router";
import { PageLayout } from "./layout/PageLayout";

export function NotFoundPage() {
  return (
    <PageLayout title="Not found" subtitle="There is no view at this address.">
      <Link className="text-sky-700 underline" to="/hq">
        Go to the HQ view
      </Link>
    </PageLayout>
  );
}
