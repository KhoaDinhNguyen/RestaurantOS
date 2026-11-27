import { Navigate, type RouteObject } from "react-router";
import { NotFoundPage } from "./pages/NotFoundPage";
import { RootLayout } from "./pages/layout/RootLayout";

// The four views are rendered by RootLayout, which keeps all of them mounted
// so each one keeps its state across tab switches. Their routes are listed
// here only so the router knows the paths.
export const routes: RouteObject[] = [
  {
    path: "/",
    element: <RootLayout />,
    children: [
      { index: true, element: <Navigate to="/hq" replace /> },
      { path: "hq", element: null },
      { path: "region", element: null },
      { path: "customer", element: null },
      { path: "ops", element: null },
      { path: "*", element: <NotFoundPage /> },
    ],
  },
];
