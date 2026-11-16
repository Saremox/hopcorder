import { expect, test, type Page } from '@playwright/test'
import { CYCLES, DEST, TARGET, seed } from './seed'

// Runs against a server with the embedded UI, ClickHouse behind it and
// e2e/asn.tsv as its ASN database (see the ui job in .github/workflows/ci.yml).

const agent = `e2e-${Date.now().toString(36)}`
const streamHash = `#/stream?agent=${agent}&target=${TARGET}&protocol=icmp&range=6h`

test.beforeAll(async ({ baseURL }) => {
  await seed(baseURL ?? 'http://127.0.0.1:8080/', agent, Date.now() - 60_000)
})

/** Fails the test on any console error, e.g. a Content-Security-Policy violation. */
function watchErrors(page: Page): string[] {
  const errors: string[] = []
  page.on('console', (m) => {
    if (m.type() === 'error') errors.push(m.text())
  })
  page.on('pageerror', (e) => errors.push(e.message))
  return errors
}

test('stream list links to the stream', async ({ page }) => {
  const errors = watchErrors(page)
  await page.goto('./')
  await expect(page).toHaveURL(/\/ui\/$/)
  await page.getByLabel('Filter streams').fill(agent)
  const row = page.locator('.stream-table tbody tr')
  await expect(row).toHaveCount(1)
  await expect(row).toContainText('icmp')
  await row.getByRole('link', { name: TARGET }).click()
  await expect(page).toHaveURL(new RegExp(`agent=${agent}`))
  await expect(page.locator('.stats')).toContainText(String(CYCLES))
  expect(errors).toEqual([])
})

test('topology, scrubber and events', async ({ page }) => {
  const errors = watchErrors(page)
  await page.goto(`./ui/${streamHash}`)

  // Summary: the route change and the loop's two; the outage and the loop's
  // unreached cycles; two looping addresses in three cycles.
  const stats = page.locator('.stats')
  await expect(stats).toContainText(`Cycles${CYCLES}`)
  await expect(stats).toContainText('Route changes3')
  await expect(stats).toContainText('Outages2')
  await expect(stats).toContainText('Loops6')

  // The graph is drawn, with the ASes from e2e/asn.tsv in its legend.
  const canvas = page.locator('.graph canvas').first()
  await expect(canvas).toBeVisible()
  expect((await canvas.boundingBox())?.width ?? 0).toBeGreaterThan(400)
  await expect(page.locator('.legend')).toContainText('AS64496 TRANSIT-A')
  await expect(page.locator('.legend')).toContainText('AS64499 TRANSIT-D')
  await expect(page.locator('.graph-panel .panel-head')).toContainText('addresses over 8 TTLs')

  // The latest cycle reached the destination over the new route.
  const cycle = page.locator('section.cycle')
  await expect(cycle.locator('.badge')).toHaveText('reached')
  const rows = cycle.locator('tbody tr')
  await expect(rows).toHaveCount(6)
  await expect(rows.nth(5)).toContainText(DEST)
  await expect(rows.nth(5)).toContainText('AS64498 HOSTING-C')
  await expect(rows.nth(4)).toContainText('192.0.2.17')
  await page.screenshot({ path: 'test-results/stream.png', fullPage: true })

  // Step back one cycle.
  const seq = Number((await cycle.locator('.panel-head .mono').textContent())?.replace('#', ''))
  await page.getByRole('button', { name: 'Previous cycle' }).click()
  await expect(cycle.locator('.panel-head .mono')).toHaveText(`#${seq - 1}`)
  await expect(page).toHaveURL(/at=\d+/)

  // The route change an hour ago: TTL 5 moved from IX-B to TRANSIT-D.
  await page.getByRole('tab', { name: /Route changes/ }).click()
  await page.locator('.event-list button').last().click()
  await expect(page.locator('.change-note')).toContainText('TTL 5: 198.51.100.130 → 192.0.2.17')
  await expect(cycle.locator('tr.changed')).toContainText('192.0.2.17')
  await page.screenshot({ path: 'test-results/change.png', fullPage: true })

  // The loop.
  await page.getByRole('tab', { name: /Loops/ }).click()
  await page.locator('.event-list button').first().click()
  await expect(page.locator('.loop-note')).toHaveText('Loop: 198.51.100.9 at TTL 4, 6, 8; 192.0.2.17 at TTL 5, 7')
  await expect(cycle.locator('tr.loop')).toHaveCount(5)
  await expect(cycle.locator('.badge')).toHaveText('not reached')
  await page.screenshot({ path: 'test-results/loop.png', fullPage: true })

  // The five-minute outage ended after TTL 4; the newest is the loop's.
  await page.getByRole('tab', { name: /Outages/ }).click()
  await expect(page.locator('.event-list')).toContainText('5m')
  await expect(page.locator('.event-list')).toContainText('last answer TTL 4 198.51.100.9')
  await page.locator('.event-list button').first().click()
  await expect(cycle.locator('.badge')).toHaveText('not reached')

  // Clicking the timeline selects that moment; "latest" follows again.
  const tl = page.locator('.timeline canvas').first()
  const box = await tl.boundingBox()
  if (!box) throw new Error('no timeline')
  await page.mouse.click(box.x + box.width * 0.9, box.y + box.height / 2)
  await expect(page.locator('.live')).toHaveText('latest')
  await page.locator('.live').click()
  await expect(page.locator('.live')).toHaveText('following latest')
  await expect(page).not.toHaveURL(/at=/)

  expect(errors).toEqual([])
})

test('an incomplete stream link shows the list', async ({ page }) => {
  await page.goto('./ui/#/stream?agent=&target=x')
  // Without an agent the hash is not a stream: the list is shown.
  await expect(page.getByRole('heading', { name: 'Streams' })).toBeVisible()
})
