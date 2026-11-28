import { createContext } from "react";

/**
 * Whether the view around a component is the one on screen. Every view stays
 * mounted so it keeps its state when you switch tabs; a hidden one is
 * marked inactive and stops polling. Outside any view it is always true.
 */
export const ViewActiveContext = createContext(true);
