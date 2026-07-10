---
name: Feature request
about: Suggest an idea or improvement
title: ''
labels: enhancement
assignees: ''
---

**Problem**
What are you trying to do that interlock makes hard or impossible today?

**Proposed solution**
What you'd like to see. API sketches welcome.

**Alternatives considered**
Other approaches you weighed.

**Scope check**
interlock is deliberately focused: a circuit breaker core, a composable
[resilience pipeline](https://bagowix.github.io/interlock/guides/pipeline/)
and thin [integrations](https://bagowix.github.io/interlock/integrations/)
on native extension points. New integrations are prioritised by demand —
an issue like this one is exactly the signal we look for. Retry stays
delegated to tenacity; things we consider out of scope: caching, hedging
(for now), own retry/backoff engines.
