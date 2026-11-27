import { useContext } from "react";
import { ViewActiveContext } from "../pages/layout/viewActiveContext";

/** True while the view this component belongs to is on screen. */
export function useViewActive(): boolean {
  return useContext(ViewActiveContext);
}
