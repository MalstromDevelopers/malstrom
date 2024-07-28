import {defineConfig} from 'vite';
import motionCanvas from '@motion-canvas/vite-plugin';
import ffmpeg from '@motion-canvas/ffmpeg';

export default defineConfig({
  plugins: [
    motionCanvas(),
    ffmpeg(),
  ],
  build: { 
    rollupOptions: { 
      output: { 
        dir: './dist/animations', 
        entryFileNames: '[name].js', 
      }, 
    }, 
  }, 
});
