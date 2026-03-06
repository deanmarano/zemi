import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

const sidebars: SidebarsConfig = {
  zemiSidebar: [
    'home',
    'zemi/overview',
    'zemi/benchmarks',
    'zemi/architecture',
    'zemi/configuration',
    {
      type: 'category',
      label: 'PostgreSQL Setup',
      collapsed: false,
      items: [
        'postgresql/source-database',
        {
          type: 'category',
          label: 'Hosting Platforms',
          collapsed: true,
          items: [
            'hosting/supabase',
            'hosting/neon',
            'hosting/aws',
            'hosting/gcp',
            'hosting/render',
            'hosting/digitalocean',
            'hosting/self-managed',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'ORM Packages',
      collapsed: true,
      items: [
        {
          type: 'category',
          label: 'JavaScript/TypeScript',
          collapsed: true,
          items: [
            'orms/prisma',
            'orms/drizzle',
            'orms/typeorm',
            'orms/supabase-js',
            'orms/mikro-orm',
          ],
        },
        {
          type: 'category',
          label: 'Ruby',
          collapsed: true,
          items: [
            'orms/rails',
          ],
        },
        {
          type: 'category',
          label: 'Python',
          collapsed: true,
          items: [
            'orms/sqlalchemy',
            'orms/django',
          ],
        },
      ],
    },
    'alternatives',
    'zemi/migration',
  ],
  bemiSidebar: [
    {
      type: 'category',
      label: 'ORM Packages',
      collapsed: false,
      items: [
        {
          type: 'category',
          label: 'JavaScript/TypeScript',
          collapsed: false,
          items: [
            'orms/prisma',
            'orms/drizzle',
            'orms/typeorm',
            'orms/supabase-js',
            'orms/mikro-orm',
          ],
        },
        {
          type: 'category',
          label: 'Ruby',
          collapsed: false,
          items: [
            'orms/rails',
          ],
        },
        {
          type: 'category',
          label: 'Python',
          collapsed: false,
          items: [
            'orms/sqlalchemy',
            'orms/django',
          ],
        },
      ],
    },
  ],
};

export default sidebars;
