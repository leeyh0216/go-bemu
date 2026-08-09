import { expect, test } from "@playwright/test";

test("query, results and jobs remain connected", async ({ page }) => {
  await page.goto("");
  const run = page.getByRole("button", { name: "Run" });
  await expect(run).toBeEnabled();
  await run.click();
  await expect(page.getByText("page_view")).toBeVisible();
  await page.getByRole("button", { name: "Jobs" }).click();
  await expect(page.getByRole("heading", { name: "Jobs" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "QUERY", exact: true }).first()).toBeVisible();
});

test("table explorer exposes schema and preview", async ({ page, isMobile }) => {
  await page.goto("");
  if (isMobile) await page.getByLabel("Show explorer").click();
  await page.getByText("events", { exact: true }).click();
  await expect(page.getByRole("heading", { name: "events" })).toBeVisible();
  await expect(page.getByText("event_id")).toBeVisible();
  await page.getByRole("tab", { name: "Preview" }).click();
  await expect(page.getByText("evt_10291")).toBeVisible();
});
