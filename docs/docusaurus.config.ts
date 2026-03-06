import { themes as prismThemes } from "prism-react-renderer";
import type { Config } from "@docusaurus/types";
import type * as Preset from "@docusaurus/preset-classic";

const config: Config = {
  title: "Zemi",
  tagline: "Automatic PostgreSQL change tracking",
  favicon: "img/favicon.ico",
  url: "https://deanmarano.github.io",
  baseUrl: "/zemi/",
  organizationName: "deanmarano",
  projectName: "zemi",
  trailingSlash: false,
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",

  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  clientModules: [
    './src/js/clientModule.js',
  ],

  themes: [
    [
      require.resolve("@easyops-cn/docusaurus-search-local"),
      {
        hashed: true,
        language: ["en"],
        docsRouteBasePath: '/',
      },
    ],
  ],

  presets: [
    [
      "classic",
      {
        docs: {
          routeBasePath: "/",
          sidebarPath: "./sidebars.ts",
          editUrl: "https://github.com/deanmarano/zemi/tree/main/docs/",
        },
        blog: false,
        theme: {
          customCss: "./src/css/custom.css",
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    metadata: [
      {name: 'title', content: 'Zemi Docs - Automatic PostgreSQL Change Tracking'},
      {name: 'description', content: 'Zemi: Automatic, reliable PostgreSQL change tracking via WAL replication. A single static binary that captures every INSERT, UPDATE, DELETE, and TRUNCATE with context-aware audit trails.'},
      {name: 'keywords', content: 'Zemi, PostgreSQL audit trail, database tracking, change data capture, WAL replication, logical replication, pgoutput, Zig'},
      {name: 'image', content: 'img/social-card.png'},
    ],
    image: "img/social-card.png",
    navbar: {
      title: "Zemi",
      logo: {
        alt: "Zemi Logo",
        src: "img/logo.png",
      },
      items: [
        {
          type: "docSidebar",
          sidebarId: "zemiSidebar",
          label: "Docs",
          position: "left",
        },
        {
          type: "docSidebar",
          sidebarId: "bemiSidebar",
          label: "Bemi ORM Packages",
          position: "left",
        },
        {
          href: "https://github.com/deanmarano/zemi",
          label: "GitHub",
          position: "right",
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Zemi",
          items: [
            {
              label: "Overview",
              href: "/zemi/zemi",
            },
            {
              label: "Benchmarks",
              href: "/zemi/zemi/benchmarks",
            },
            {
              label: "Architecture",
              href: "/zemi/zemi/architecture",
            },
            {
              label: "Configuration",
              href: "/zemi/zemi/configuration",
            },
            {
              label: "Migration Guide",
              href: "/zemi/zemi/migration",
            },
          ],
        },
        {
          title: "ORM Packages",
          items: [
            {
              label: "Prisma",
              href: "https://github.com/BemiHQ/bemi-prisma",
            },
            {
              label: "Ruby on Rails",
              href: "https://github.com/BemiHQ/bemi-rails",
            },
            {
              label: "TypeORM",
              href: "https://github.com/BemiHQ/bemi-typeorm",
            },
            {
              label: "SQLAlchemy",
              href: "https://github.com/BemiHQ/bemi-sqlalchemy",
            },
            {
              label: "Supabase JS",
              href: "https://github.com/BemiHQ/bemi-supabase-js",
            },
            {
              label: "MikroORM",
              href: "https://github.com/BemiHQ/bemi-mikro-orm",
            },
            {
              label: "Django",
              href: "https://github.com/BemiHQ/bemi-django",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "GitHub",
              href: "https://github.com/deanmarano/zemi",
            },
            {
              label: "Discord",
              href: "https://discord.gg/mXeZ6w2tGf",
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Zemi`,
    },
    prism: {
      theme: prismThemes.github,
      darkTheme: prismThemes.dracula,
      additionalLanguages: ['ruby', 'zig'],
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
