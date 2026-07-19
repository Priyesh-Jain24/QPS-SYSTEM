import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/search': 'http://localhost:8080',
      '/suggest': 'http://localhost:8080',
      '/analytics': 'http://localhost:8080',
      '/health': 'http://localhost:8080',
      '/metrics': 'http://localhost:8080',
      '/index': 'http://localhost:8080',
      '/bulk': 'http://localhost:8080'
    }
  }
})
