# DESIGN.md — freemodel-cc-proxy UI

A documented design system for the local proxy console, in the
[DESIGN.md](https://designmd.me) format (tokens + typography + components + rules).
The UI is a single `ui.html` that implements these tokens directly as CSS variables.

## Personality

**Graphite instrument console.** A calm, precise developer console for a local
Claude proxy: near-black neutral graphite surfaces, **one warm amber signal** used
sparingly (the active key + the primary action), tabular monospace for every number,
quiet semantic dots, and a lot of air. Restraint is the whole point — color is a
signal, not decoration.

Reference vibes: Vercel / Raycast / Linear dashboards, warmed toward Claude.

## Design principles (audit-grounded)

1. **Depth by background contrast + soft shadow, not heavy borders.** Borders are
   thin, low-contrast, and used as hairline separators, never as the main frame.
2. **Hierarchy through size & contrast, not color.** Muted ink by default; ink-2 for
   values; one accent reserved for the active/primary state.
3. **8 px spacing system.** All gaps and paddings are multiples of 4.
4. **Tabular figures everywhere.** Every number (counts, tokens, statuses, times)
   uses `font-feature-settings: "tnum" 1` so columns align.
5. **Not color alone.** Status has a dot + a word + a shape, so it reads without color
   (a11y).
6. **Calm by default, motion as feedback only.** Subtle transitions on hover/focus;
   `prefers-reduced-motion` disables all of them.

## Color tokens (oklch)

```css
/* surfaces — elevation by lightness, near-neutral cool graphite */
--bg:        oklch(16% 0.006 264);   /* page */
--panel:     oklch(20% 0.007 264);  /* card */
--panel-2:   oklch(24% 0.008 264);  /* inset / input */
--panel-3:   oklch(28% 0.009 264);  /* raised (selected, hover) */

/* lines — hairline, low-alpha */
--line:      oklch(36% 0.008 264 / 55%);
--line-2:    oklch(44% 0.010 264 / 35%);

/* ink — a 4-step ramp (muted → strong) */
--ink:       oklch(97% 0.002 264);
--ink-2:     oklch(80% 0.004 264);   /* values */
--ink-3:     oklch(62% 0.006 264);   /* labels, secondary */
--ink-4:     oklch(50% 0.008 264);   /* faint */

/* accent — the single warm signal */
--accent:    oklch(76% 0.13 70);     /* warm amber */
--accent-ink:oklch(22% 0.03 70);     /* text on accent */
--accent-soft:oklch(76% 0.13 70 / 12%);

/* semantic — muted, used as dots */
--ok:   oklch(75% 0.12 152);   /* green */
--warn: oklch(78% 0.13 75);    /* amber */
--err:  oklch(68% 0.18 22);    /* red */
--info: oklch(72% 0.10 240);   /* blue */
```

Accent usage rule: **accent is reserved for (a) the active/current key and (b) the
primary button.** Everything else is neutral. Semantic colors appear only as small
status dots + a word, never as large fills.

## Typography

```css
--sans: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
--mono: ui-monospace, "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
--tnum: "tnum" 1, "lnum" 1;   /* tabular lining figures */
```

| Role        | Size | Weight | Family | Notes |
|-------------|------|--------|--------|-------|
| Section label | 11px | 600 | sans | uppercase, tracked +.06em, `--ink-3` |
| Body | 13px | 400 | sans | `--ink-2` |
| Value (mono) | 13px | 500 | mono | `--ink-2`, tnum |
| Card title | 15px | 650 | sans | `--ink`, tracking −.01em |
| Metric number | 22px | 600 | mono | `--ink`, tnum, tracking −.01em |
| Metric caption | 11.5px | 400 | mono | `--ink-4`, tnum |
| Code | 12px | 400 | mono | `--ink-2` |

Line-height 1.5 for body, 1.25 for large numbers.

## Spacing & radius

```css
--sp-1:4px; --sp-2:8px; --sp-3:12px; --sp-4:16px; --sp-5:20px; --sp-6:24px;
--r-card: 12px; --r-ctrl: 8px; --r-pill: 999px;
--shadow: 0 1px 2px oklch(0% 0 0 / .30), 0 6px 20px oklch(0% 0 0 / .18);
```

## Components

- **Card** — `--panel`, `--line` hairline, `--r-card`, soft `--shadow`. Title = section label.
- **Metric** — small label over a big tabular number; `--panel` surface; lifts 1px on hover.
- **Key row** — mono, tabular counts. Active row: left inset accent bar + faint
  `--accent-soft` tint. Status = a dot + word (`активен` / `битый` / `лимит` / `ок`).
- **Status dot** — 7px circle in the semantic color; pulsing only for the live health
  indicator in the header.
- **Button** — primary: `--accent` fill, `--accent-ink` text. Ghost: `--panel-2` +
  `--line-2`. Danger-ghost: transparent + `--err` text.
- **Input** — `--panel-2`, `--line`, focus = accent ring (`box-shadow: 0 0 0 3px --accent-soft`).
- **Segmented control** — pill of buttons in `--panel-2`; selected = `--panel-3` + shadow.
- **Code block** — `oklch(14% ...)` inset, mono, keywords `--accent`, comments `--ink-4`.
- **Toggle (profile enable)** — 42×24 pill track + 18px knob; off = `--panel-2`/`--ink-3`,
  on = `--accent-soft` track + `--accent` knob translated 18px. Keyboard-accessible
  (`role=switch`, Space/Enter). Sits in the header so the profile state is always visible.
- **Routing diagram** — the "backend logic" card. A vertical flow of `--panel-2` nodes
  connected by `→`/`↓` arrows: client → protocol choice → model-id routing → two
  family-colored branches (`--claude` warm / `--gpt` blue) ending at their upstream.
  Family `pill` tags reuse the family colors. This is the single source of truth for
  what the proxy does, drawn so a reader gets it in 3 seconds.
- **Family pills** — small mono pills: `.pill.claude` (warm border+text on
  `--claude-soft`), `.pill.gpt` (blue), `.pill.muted` (neutral). Used in the diagram,
  model groups, and model-id routing row.

## Two-family color extension

The console fronts **two** model families, so the single-accent rule gets one
exception: a second, blue signal for the GPT family, used only as a family tag
(border + small text + soft tint), never as a large fill or a primary action.

```css
--claude:      oklch(76% 0.13 70);   /* warm — Claude family (same hue as accent) */
--claude-soft: oklch(76% 0.13 70 / 12%);
--gpt:         oklch(72% 0.10 240);  /* blue — GPT family */
--gpt-soft:    oklch(72% 0.10 240 / 12%);
```

Amber stays the accent (active key, primary button, profile ON). Blue is a
**family label only**, not a second accent — it never appears on buttons or the
active state.

## Profile concept

FreeModel is a **profile**: a named, toggleable provider configuration (id, name,
enabled, note, upstreams). The header carries an ON/OFF toggle that PUTs
`/api/profile { enabled }`. When off, `/v1/*` returns `503 profile_disabled`
with a clear message; the UI and `/api/*` stay up so you can re-enable. The
profile card shows the routing diagram (above) so the toggle's effect is obvious.

## Localization

Default language: **Russian** (UI labels, status words, empty states, toasts).
Technical identifiers stay Latin: model ids, endpoint paths, `fe_oa_` prefix, HTTP
methods, header names.

## Implementation

The single file `ui.html` defines all tokens as `:root` CSS variables and consumes
them everywhere — no inline hex values in markup. Changing a token in one place
re-themes the whole console.
