import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Malstrom",
  description: "Malstrom - Stateful, Distributed Stream Processing",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
    ],

    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/GettingStarted' },
          { text: 'Keyed Streams', link: '/guide/KeyedStreams' },
          { text: 'Stateful Programs', link: '/guide/StatefulPrograms' },
          { text: 'Joining and Splitting Streams', link: '/guide/JoiningSplitting' },
          { text: 'Timely Processing', link: '/guide/TimelyProcessing' },
          { text: 'Deploying to Kubernetes', link: '/guide/Kubernetes' },
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/MalstromDevelopers/jetstream' }
    ]
  }
})
