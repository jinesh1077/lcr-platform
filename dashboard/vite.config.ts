import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3000,
    proxy: {
      '/ingestion': { target: 'http://localhost:8080', rewrite: (p) => p.replace(/^\/ingestion/, '') },
      '/routing': { target: 'http://localhost:8081', rewrite: (p) => p.replace(/^\/routing/, '') },
      '/telemetry': { target: 'http://localhost:8082', rewrite: (p) => p.replace(/^\/telemetry/, '') },
      '/mock': { target: 'http://localhost:8083', rewrite: (p) => p.replace(/^\/mock/, '') },
    },
  },
})
