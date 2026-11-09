<template>
  <main ref="scroller" class="grow min-h-0 overflow-y-auto bg-default">
    <div class="mx-auto w-full max-w-screen-xl flex gap-8 px-6 py-8">
      <nav class="hidden md:block w-60 shrink-0 sticky top-0 self-start text-sm" aria-label="User guide pages">
        <NuxtLink to="/guide" class="block font-semibold mb-3 hover:text-primary" :class="{ 'text-primary': slug === 'README' }">
          User guide
        </NuxtLink>
        <ol class="flex flex-col gap-1">
          <li v-for="p in pages" :key="p.slug">
            <NuxtLink :to="`/guide/${p.slug}`" class="block py-0.5 text-muted hover:text-default"
              :class="{ '!text-primary font-medium': p.slug === slug }">
              {{ p.title }}
            </NuxtLink>
          </li>
        </ol>
      </nav>

      <article class="min-w-0 grow max-w-4xl">
        <template v-if="html">
          <video v-if="slug !== 'README'" :key="slug" class="block w-full aspect-video rounded-lg mb-6 bg-black"
            :src="`${videoBase}${slug}.mp4`" :poster="`${videoBase}${slug}.webp`" controls playsinline preload="metadata" />
          <!-- eslint-disable-next-line vue/no-v-html -- trusted Markdown bundled with the app -->
          <div class="guide-content" @click="onClick" v-html="html" />
        </template>
        <div v-else class="text-muted">
          This page doesn't exist. <NuxtLink to="/guide" class="text-primary">See all guide pages</NuxtLink>.
        </div>
      </article>
    </div>
  </main>
</template>

<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue'
import { Marked } from 'marked'

/**
 * The user guide: one Markdown page per tutorial clip, kept in `content/guide/` so it also reads on GitHub.
 * Links between pages are written as `NN-topic.md` (GitHub-style) and rewritten to `/guide/NN-topic` here.
 */
const sources = import.meta.glob('../../../content/guide/*.md', { query: '?raw', import: 'default', eager: true }) as Record<string, string>
const bySlug: Record<string, string> = Object.fromEntries(
  Object.entries(sources).map(([path, md]) => [path.split('/').pop()!.replace(/\.md$/, ''), md]),
)

const pages = Object.keys(bySlug).filter((s) => s !== 'README').sort().map((s) => ({
  slug: s,
  title: (bySlug[s]!.match(/^# (.+)$/m)?.[1] ?? s).trim(),
}))

const route = useRoute()
const router = useRouter()
const slug = computed(() => (route.params.slug as string | undefined) || 'README')
const videoBase = useRuntimeConfig().public.guideVideoBase as string

// GitHub's heading anchors, so `page.md#section` links work in both places.
const anchor = (text: string) => text.toLowerCase().replace(/<[^>]+>/g, '').replace(/[^\p{L}\p{N}\s-]/gu, '').trim().replace(/\s/g, '-')

const marked = new Marked({
  renderer: {
    heading({ tokens, depth, text }) {
      return `<h${depth} id="${anchor(text)}">${this.parser.parseInline(tokens)}</h${depth}>\n`
    },
    link({ href, tokens }) {
      const inner = this.parser.parseInline(tokens)
      const m = href.match(/^([\w-]+)\.md(#.*)?$/)
      if (m) return `<a href="/guide${m[1] === 'README' ? '' : `/${m[1]}`}${m[2] ?? ''}">${inner}</a>`
      if (href.startsWith('#')) return `<a href="${href}">${inner}</a>`
      return `<a href="${href}" target="_blank" rel="noopener noreferrer">${inner}</a>`
    },
  },
})

const html = computed(() => {
  const md = bySlug[slug.value]
  return md ? (marked.parse(md) as string) : ''
})

useHead(() => ({
  title: `${slug.value === 'README' ? 'User guide' : pages.find((p) => p.slug === slug.value)?.title ?? 'User guide'} · OceanECO`,
}))

// In-guide links navigate client-side; hash links scroll the page's own scroller.
function onClick(e: MouseEvent) {
  const a = (e.target as HTMLElement).closest('a')
  const href = a?.getAttribute('href')
  if (!href || a!.target === '_blank' || e.ctrlKey || e.metaKey || e.shiftKey) return
  if (href.startsWith('/guide') || href.startsWith('#')) {
    e.preventDefault()
    router.push(href.startsWith('#') ? { hash: href } : href)
  }
}

const scroller = ref<HTMLElement | null>(null)
watch(() => route.fullPath, async () => {
  await nextTick()
  const target = route.hash ? document.getElementById(decodeURIComponent(route.hash.slice(1))) : null
  if (target) target.scrollIntoView({ block: 'start' })
  else scroller.value?.scrollTo({ top: 0 })
}, { immediate: true })
</script>

<style scoped>
.guide-content {
  line-height: 1.7;
}
.guide-content :deep(h1) { font-size: 2rem; font-weight: 700; margin: 0 0 0.5rem; }
.guide-content :deep(h2) { font-size: 1.4rem; font-weight: 600; margin: 2.2rem 0 0.6rem; padding-bottom: 0.3rem; border-bottom: 1px solid var(--ui-border); }
.guide-content :deep(h3) { font-size: 1.1rem; font-weight: 600; margin: 1.6rem 0 0.4rem; }
.guide-content :deep(h2), .guide-content :deep(h3) { scroll-margin-top: 1rem; }
.guide-content :deep(p) { margin: 0.7rem 0; }
.guide-content :deep(ul), .guide-content :deep(ol) { margin: 0.7rem 0; padding-left: 1.5rem; }
.guide-content :deep(ul) { list-style: disc; }
.guide-content :deep(ol) { list-style: decimal; }
.guide-content :deep(li) { margin: 0.25rem 0; }
.guide-content :deep(li > ul), .guide-content :deep(li > ol) { margin: 0.25rem 0; }
.guide-content :deep(a) { color: var(--ui-primary); text-underline-offset: 3px; }
.guide-content :deep(a:hover) { text-decoration: underline; }
.guide-content :deep(strong) { font-weight: 600; color: var(--ui-text-highlighted); }
.guide-content :deep(code) { font-family: monospace; font-size: 0.9em; padding: 0.1em 0.35em; border-radius: 4px; background: var(--ui-bg-elevated); }
.guide-content :deep(blockquote) { margin: 1rem 0; padding: 0.6rem 1rem; border-left: 3px solid var(--ui-primary); background: var(--ui-bg-elevated); border-radius: 0 6px 6px 0; }
.guide-content :deep(blockquote p) { margin: 0; }
.guide-content :deep(table) { width: 100%; margin: 1rem 0; border-collapse: collapse; font-size: 0.95em; display: block; overflow-x: auto; }
.guide-content :deep(th), .guide-content :deep(td) { padding: 0.45rem 0.75rem; border: 1px solid var(--ui-border); text-align: left; vertical-align: top; }
.guide-content :deep(td:last-child a[href^="/guide/"]) { white-space: nowrap; }
.guide-content :deep(th) { background: var(--ui-bg-elevated); font-weight: 600; }
</style>
