import { fileURLToPath } from 'node:url'
import vue from '@vitejs/plugin-vue'
import { defineConfig } from 'vitest/config'

// The build goes to internal/ui/dist, which the Go binary embeds and serves
// under /ui/. Asset URLs are relative ('./'), so the UI also works behind a
// reverse proxy that mounts hopcorder under a prefix.
export default defineConfig({
  plugins: [vue()],
  base: './',
  build: {
    outDir: fileURLToPath(new URL('../internal/ui/dist', import.meta.url)),
    emptyOutDir: true,
    target: 'es2022',
    chunkSizeWarningLimit: 1200,
    reportCompressedSize: false,
  },
  server: {
    // npm run dev: the read API of a server running on the default port.
    proxy: { '/api': 'http://127.0.0.1:8080' },
  },
  test: {
    include: ['tests/**/*.test.ts'],
  },
})
