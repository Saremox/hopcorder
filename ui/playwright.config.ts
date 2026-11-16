import { defineConfig } from '@playwright/test'

// End-to-end tests against a running server (see e2e/ui.spec.ts):
//   HOPCORDER_URL=http://127.0.0.1:8080/ npm run e2e
export default defineConfig({
  testDir: 'e2e',
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  reporter: [['list']],
  use: {
    baseURL: process.env.HOPCORDER_URL ?? 'http://127.0.0.1:8080/',
    viewport: { width: 1440, height: 1000 },
    trace: 'off',
  },
})
