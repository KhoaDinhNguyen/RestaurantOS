import type { ReactNode } from "react";

/** The frame every view uses: a title, what the view stands for, and its panels. */
export function PageLayout({
  title,
  subtitle,
  toolbar,
  children,
}: {
  title: string;
  subtitle: string;
  toolbar?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-xl font-semibold">{title}</h1>
          <p className="text-sm text-slate-500">{subtitle}</p>
        </div>
        {toolbar}
      </div>
      {children}
    </div>
  );
}
