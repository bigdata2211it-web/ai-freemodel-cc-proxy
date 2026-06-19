# Research: clean separation of providers (backend + UI)

> 5 focused investigations of real-world patterns for splitting two distinct
> backends (FreeModel, OpenCode Go) so neither the code nor the UI interweave
> them. Goal: each provider is a self-contained **space** — its own module in
> the backend, its own section in the UI — sharing only a thin shell.
> Studied natively (what the user/UX feels), not just technically.

---

## Pass 1 — Provider-adapter pattern (LiteLLM / Vercel AI SDK / aisuite)

**Native idea:** a provider is a *thing with a personality*, not a `kind` string
you branch on. LiteLLM and the Vercel AI SDK model each provider as an object
that owns: how to route a call, how to auth it, how to list its models, how to
shape the request for its wire format. The orchestrator never says
`if provider.kind === "freemodel"`; it says `provider.route(...)`. Each
provider's quirks (fingerprint gate, baseUrl, special headers) live **inside**
that provider, not in a shared switch.

**Symptom in our code today:** `routeRequest`, `modelsEndpoints`,
`probeProviderOnce`, `defaultProviders`, and the `/v1/*` body-prep all branch on
`p.kind === "freemodel"`. FreeModel's fingerprint logic and OpenCode's
baseUrl logic sit in the same functions. That is the interweaving.

**Pattern to adopt:** a `Provider` interface with methods `route()`,
`headersFor()`, `prepareAnthropicBody()`, `prepareOpenAIBody()`,
`listModelsEndpoints()`, `probeKey()`. Two implementations:
`FreeModelProvider` and `OpenAICompatProvider`. Each lives in its own file.
`index.js` becomes a registry + HTTP shell that calls `provider.X()` — zero
kind-branches in the shell.

---

## Pass 2 — UI as a projection of backend state (Vercel / Linear / Railway)

**Native idea:** the dashboard has no mind of its own. It reads structured
resources from the API and renders each one 1:1. There is one source of truth
(the backend), and the UI is a *projection* of it. You never maintain a
parallel mental model in the UI (e.g. "current provider name string I'm
labeling a shared widget with").

**Symptom today:** the UI has one hero, one model list, one key pool, one test
panel — all *shared widgets that relabel* when you flip the provider selector.
The UI doesn't know FreeModel and OpenCode Go as two things; it knows "the
active provider" and repaints the same components. That feels interweaved: the
user can't see both at once, can't compare, can't tell which keys belong to
which backend without reading the small label.

**Pattern to adopt:** the UI renders **all providers as distinct resource
objects** it got from `/api/providers`. Each becomes its own
`ProviderSpace`. The active one is marked, not "shown instead of the other".
Shared widgets (header, global log, profile-enable switch) live in the shell,
not inside a provider space.

---

## Pass 3 — Sectioned / tabbed dashboards per resource (Stripe / Supabase / one-api)

**Native idea:** when a product has several parallel "things" (Stripe accounts,
Supabase projects, one-api channels), each gets a **dedicated, visually-bounded
space** — a tab or a clearly bordered panel — never a shared form whose fields
swap. one-api's "Channels" tab lists channels; each channel row opens its own
config. Supabase projects are cards you click into. The boundary is the point:
you always know which thing you're looking at.

**Symptom today:** flipping the header selector swaps the entire main content.
There's no "FreeModel space" and "OpenCode Go space"; there's one space that
*becomes* whichever is active. The two backends are visually invisible relative
to each other.

**Pattern to adopt:** **tabs** — `FreeModel | OpenCode Go` — each tab is a full
`ProviderSpace` (hero, routing diagram, models, keys, test, fingerprint-if-any).
Both always exist as distinct destinations. The "active" provider (which one
`/v1/*` routes to) is a toggle *inside* each space ("Сделать активным" /
"Активен"), decoupled from which tab you're viewing — so you can manage
OpenCode Go's keys while FreeModel is the active route.

---

## Pass 4 — Component composition = slot/card mapping 1:1 to a backend resource

**Native idea:** a component = a resource. A `ProviderSpace` composes
`ProviderHero`, `RoutingDiagram`, `ModelList`, `KeyPool`, `TestPanel`,
`FingerprintEditor` — all scoped to *one* provider instance. The same component
class is instantiated per provider; instances never share state. The shell
composes `Header + ProviderTabs([]ProviderSpace) + GlobalLog`. Composition is
the boundary.

**Symptom today:** the components are singletons keyed by fixed DOM ids
(`#modelsList`, `#keys`, `#profileToggle`). There's one of each. You cannot
have two key-pool tables on the page. The architecture hard-codes "one
provider visible".

**Pattern to adopt:** `ProviderSpace(provider)` renders into a container per
provider (`#space-freemodel`, `#space-opencode`) with ids namespaced by
provider id (`#keys-freemodel`, `#models-freemodel`, …). Shared shell keeps
global ids (`#pulse`, `#badge`, `#logs`). Tab switch = show/hide a space, not
re-render one space with different data.

---

## Pass 5 — Native UX: the UI speaks each backend's vocabulary

**Native idea:** a good console uses the *noun's own words*, not generic
technical labels. FreeModel is "Real Claude through the Claude Code client
gate" — its section should say that, show the fingerprint, name `cc.freemodel.dev`.
OpenCode Go is "your OpenCode Go subscription" — its section should say *that*,
name `opencode.ai/zen/go/v1`, list glm/kimi/deepseek as "the models your plan
serves". The UI *understands* what each backend is; it doesn't say
"kind: openai-compat, baseUrl: …" like a config dump.

**Symptom today:** hero meta shows `kind: openai-compat` and a generic
`upstream baseUrl` row. The fingerprint card is hidden for OpenCode but the
hero still has "claude upstream / openai upstream" rows that read "—" for
OpenCode. It's a generic form filled in differently, not a native description.

**Pattern to adopt:** each `ProviderSpace` has its own hero copy + meta rows
that make sense for *that* provider. FreeModel hero: fingerprint + two hosts +
"Claude Code client gate". OpenCode Go hero: subscription + one endpoint +
"Bearer sk-, no gate, 20 models". The active-route toggle is a verb in the
provider's own language ("Сделать активным провайдером").

---

## Distilled principles (applied below)

- **P1 — One provider = one module (backend) + one space (UI).** No shared
  logic that branches on provider; no shared widget that relabels.
- **P2 — No `kind`-branching in the shell.** Each provider owns
  route/headers/body-prep/models/probe. `index.js` calls `provider.X()`.
- **P3 — UI renders provider resources 1:1.** A `ProviderSpace` per provider,
  each with namespaced ids; both always exist as distinct sections.
- **P4 — Global shell is separate.** Header (health, profile-enable) and the
  request log are the only shared parts. They don't belong to any provider.
- **P5 — Native vocabulary.** Each space speaks its backend's nouns, not
  generic `kind`/`baseUrl` labels. Active-route toggle is a verb inside the
  space.

## How this maps onto the refactor

- **Backend:** `providers/base.js` (shared: key-pool rotation, safe IO, error
  normalization, SSE collectors, OpenAI⇄Anthropic translators, `forwardWithRetry`),
  `providers/freemodel.js` (FreeModel driver: fingerprint + two hosts),
  `providers/openai-compat.js` (OpenCode Go driver: one baseUrl). `index.js` =
  registry + HTTP shell + UI/API, no kind-branches.
- **UI:** tabbed `ProviderSpace` per provider — FreeModel tab and OpenCode Go
  tab, each self-contained (hero, routing diagram, models, keys, test,
  fingerprint for FreeModel). Shared shell: header + global log. Active-route
  toggle lives inside each space.
