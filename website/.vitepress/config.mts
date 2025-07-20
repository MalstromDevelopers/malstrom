import { defineConfig } from 'vitepress'
import footnote from 'markdown-it-footnote'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Malstrom",
  description: "Malstrom - Stateful, Distributed Stream Processing",
  base: "/malstrom/",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
    ],

    sidebar: [
      {
        text: 'About Malstrom',
        items: [
          { text: 'What is Malstrom?', link: '/WhatIsMalstrom' },
        ]
      },
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/GettingStarted' },
          { text: 'Keyed Streams', link: '/guide/KeyedStreams' },
          { text: 'Stateful Programs', link: '/guide/StatefulPrograms' },
          { text: 'Joining and Splitting Streams', link: '/guide/JoiningSplitting' },
          { text: 'Timely Processing', link: '/guide/TimelyProcessing' },
          { text: 'Connecting to Kafka', link: '/guide/Kafka' },
          { text: 'Deploying to Kubernetes', link: '/guide/Kubernetes' },
          { text: 'Custom Sources', link: '/guide/CustomSources' },
          { text: 'Custom Sinks', link: '/guide/CustomSinks' },
        ]
      },
      {
        items: [
          { text: 'Malstrom compared to other frameworks', link: '/MalstromCompared' },
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/MalstromDevelopers/jetstream' }
    ]
  },
  markdown: {
    config: (md) => {
      md.use(footnote)
    }
  }
})
