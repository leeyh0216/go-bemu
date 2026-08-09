export const formatBytes = (value?: string) => {
  if (!value) return "-";
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return value;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = bytes;
  let unit = 0;
  while (size >= 1000 && unit < units.length - 1) {
    size /= 1000;
    unit += 1;
  }
  return `${size < 10 && unit > 0 ? size.toFixed(1) : Math.round(size)} ${units[unit]}`;
};

export const formatCount = (value?: string) => {
  if (!value) return "-";
  const count = Number(value);
  return Number.isFinite(count) ? new Intl.NumberFormat().format(count) : value;
};

export const formatTime = (value?: number) =>
  value ? new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "medium" }).format(value) : "-";

export const formatDuration = (start?: number, end?: number) => {
  if (!start || !end) return "-";
  const milliseconds = Math.max(0, end - start);
  if (milliseconds < 1000) return `${milliseconds} ms`;
  return `${(milliseconds / 1000).toFixed(milliseconds < 10000 ? 1 : 0)} s`;
};
