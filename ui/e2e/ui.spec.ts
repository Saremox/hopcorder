import { expect, test, type Page } from '@playwright/test'
import { CYCLES, DEST, DUAL, DUAL_CYCLES, TARGET, seed, seedDualStack } from './seed'

// Runs against a server with the embedded UI, ClickHouse behind it and
// e2e/asn.tsv as its ASN database (see the ui job in .github/workflows/ci.yml).

const agent = `e2e-${Date.now().toString(36)}`
const dualAgent = `e2d-${Date.now().toString(36)}` // not a match for the filter by agent
const streamHash = `#/stream?agent=${agent}&target=${TARGET}&protocol=icmp&flow_id=33434&range=6h`

test.beforeAll(async ({ baseURL }) => {
  await seed(baseURL ?? 'http://127.0.0.1:8080/', agent, Date.now() - 60_000)
  await seedDualStack(baseURL ?? 'http://127.0.0.1:8080/', dualAgent, Date.now() - 60_000)
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

// IPv4 and IPv6 of one name on two flows are two rows, each opening its own
// view: the IPv6 one a single ongoing outage and no route change. (On one
// flow the review counted 39 route changes and 20 outages; on two, both
// rows opened one view with both flows, 50% reached.)
test('two flows of one target are two streams', async ({ page }) => {
  const errors = watchErrors(page)
  await page.goto('./')
  const rows = page.locator('.stream-table tbody tr')
  const stats = page.locator('.stats')
  for (const [i, flow, af, reached, outages] of [
    [1, 33435, 6, '0%', 1],
    [0, 33434, 4, '100%', 0],
  ] as const) {
    await page.getByLabel('Filter streams').fill(dualAgent)
    await expect(rows).toHaveCount(2)
    await expect(rows.nth(i)).toContainText(`IPv${af} · flow ${flow}`)
    await rows.nth(i).getByRole('link', { name: DUAL }).click()
    await expect(page).toHaveURL(new RegExp(`flow_id=${flow}`))
    await expect(stats).toContainText(`Cycles${DUAL_CYCLES / 2}`)
    await expect(stats).toContainText(`Reached${reached}`)
    await expect(stats).toContainText('Route changes0')
    await expect(stats).toContainText(`Outages${outages}`)
    await expect(page.locator('.stream-head h1')).toContainText(`IPv${af} · flow ${flow}`)
    await page.goBack()
  }
  expect(errors).toEqual([])
})

// After a failed load the panels show no other stream's or range's data, and
// a failed refresh none of what it would have replaced: the error takes
// their place. Before, the view kept the previous stream's figures, graph
// and cycle under the new header, with only the error line to tell (review
// round seven).
test('a failed load shows no other stream or range', async ({ page }) => {
  const stats = page.locator('.stats')
  const cycle = page.locator('section.cycle')
  const notLoaded = async (target: string) => {
    await expect(page.locator('.stream-head h1')).toContainText(target)
    await expect(page.locator('.error')).toContainText('too many queries; retry later')
    for (const s of ['Cycles', 'Reached', 'Route changes', 'Outages', 'Loops']) await expect(stats).toContainText(`${s}–`)
    await expect(page.locator('.stream-head h1')).not.toContainText('IPv')
    await expect(cycle.locator('.panel-head')).toContainText('not loaded')
    await expect(cycle.locator('table.hops')).toHaveCount(0)
    await expect(page.locator('.graph-panel')).toContainText('Not loaded.')
    await expect(page.locator('.events')).toContainText('not loaded')
    await expect(page.locator('.events .count').first()).toHaveText('–')
  }
  const loaded = async () => {
    await expect(stats).toContainText(`Cycles${CYCLES}`)
    await expect(cycle.locator('.badge')).toHaveText('reached')
    await expect(page.locator('.error')).toHaveCount(0)
  }
  // The server's answer when both of its query slots stay busy for 10 s.
  const busy = { status: 503, contentType: 'application/json', body: '{"error":"too many queries; retry later"}' }
  const go = (hash: string) => page.evaluate((h) => (window.location.hash = h), hash) // the view stays mounted

  await page.goto(`./ui/${streamHash}`)
  await loaded()
  // Another stream, opened by a link (or back and forward).
  await page.route(new RegExp(`/api/v1/query/.*agent=${dualAgent}`), (r) => r.fulfill(busy))
  await go(`#/stream?agent=${dualAgent}&target=${DUAL}&protocol=icmp&flow_id=33435&range=6h`)
  await notLoaded(DUAL)

  // Another range.
  await go(streamHash)
  await loaded()
  await page.route(/\/api\/v1\/query\//, (r) => r.fulfill(busy))
  await page.getByRole('button', { name: '24h', exact: true }).click()
  await notLoaded(TARGET)

  // The same range again, refreshed.
  await page.unrouteAll()
  await page.getByRole('button', { name: '6h', exact: true }).click()
  await loaded()
  await page.route(/\/api\/v1\/query\//, (r) => r.fulfill(busy))
  await page.getByRole('button', { name: 'Refresh', exact: true }).click()
  await notLoaded(TARGET)
})

// A list the server cut (at the limit, 10,000 rows) says so: its count has
// a "+", and a note says only the newest are listed and counted. Before,
// 24,000 loops showed as "Loops 10000" with no note (review round five).
// Seeding past the limit takes long, so the server's answers are marked cut
// here, as the server marks them.
test('a cut list says so', async ({ page }) => {
  await page.route(/\/api\/v1\/query\/(loops|targets)\?/, async (route) => {
    const resp = await route.fetch()
    await route.fulfill({ response: resp, json: { ...(await resp.json()), truncated: true } })
  })
  await page.goto(`./ui/${streamHash}`)
  const stats = page.locator('.stats')
  await expect(stats).toContainText('Loops6+')
  await expect(stats).toContainText('Route changes3Outages2')
  await expect(page.locator('.list-note')).toHaveText(
    'Only the newest 6 loops of this range are listed and counted; there are more. Pick a shorter range for all of them.',
  )
  const tab = page.getByRole('tab', { name: /Loops/ })
  await expect(tab).toContainText('6+')
  await tab.click()
  await expect(page.locator('.events .list-end')).toHaveText('The newest 6 only; the range has more.')
  await page.getByRole('tab', { name: /Route changes/ }).click()
  await expect(page.locator('.events .list-end')).toHaveCount(0)

  await page.goto('./')
  await expect(page.locator('.streams .panel-head')).toContainText(/\d+ of \d+\+/)
  await expect(page.locator('.streams .list-note')).toContainText('more streams than the list holds')
})

// Links from before flows were part of a stream have no flow_id.
test('a link without flow_id opens its only stream', async ({ page }) => {
  await page.goto(`./ui/#/stream?agent=${agent}&target=${TARGET}&protocol=icmp&range=6h`)
  await expect(page).toHaveURL(/flow_id=33434/)
  await expect(page.locator('.stats')).toContainText(`Cycles${CYCLES}`)
})

test('a link without flow_id lists the flows it matches', async ({ page }) => {
  await page.goto(`./ui/#/stream?agent=${dualAgent}&target=${DUAL}&protocol=icmp&range=6h`)
  await expect(page.getByRole('heading', { name: 'Streams' })).toBeVisible()
  await expect(page.locator('.match-note')).toContainText(`names no single stream of ${DUAL}`)
  await expect(page.locator('.stream-table tbody tr')).toHaveCount(2)
  await expect(page).not.toHaveURL(/flow_id/)
})

test('an incomplete stream link shows the list', async ({ page }) => {
  await page.goto('./ui/#/stream?agent=&target=x')
  // Without an agent the hash is not a stream: the list is shown.
  await expect(page.getByRole('heading', { name: 'Streams' })).toBeVisible()
})
