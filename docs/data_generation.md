# Data generation strategy

This document explains how one coherent advertising ecosystem is produced, and
exactly which mechanism guarantees each property. It is the most important
document in Phase 1: the value of every later layer depends on the data being
relationally and temporally sound.

## 1. The rule everything else serves

> A child row is never created with an invented foreign key. It is created from a
> parent object that already exists.

This is enforced structurally, not by convention:

* `data_generator/relationships.py` holds the `Ecosystem` registry. Its
  `add_campaign`, `add_line_item`, `add_placement` and `add_creative` methods
  look the parent up and raise `RelationshipError` if it is missing. There is no
  code path that registers an orphan.
* The event generators receive *objects*, not identifiers. A click is built from
  a `GeneratedImpression`, so `campaign_id`, `line_item_id`, `advertiser_id` and
  `creative_id` are copied off the impression rather than re-drawn.
* `data_generator/validation.py` re-checks every row against the registry before
  it reaches a sink, and `data_quality/tests/*.sql` checks it again from the
  database's point of view after loading.

## 2. Generation order

```
advertisers ─┬─> campaigns ──> line_items
             └─> creatives
publishers ────> placements
audiences
                  │
                  └──> impressions ──> clicks ──> conversions
                                 └────────┴───────────┴──> spend_transactions
```

Publishers and placements are generated *before* creatives, because a creative
is only given a type that the generated inventory can actually serve. Generating
an AUDIO creative when no audio placement exists would create a creative that can
never appear in an impression.

## 3. How an impression is assembled

`impressions.generate_impressions_for_day` walks the graph:

| Step | Source | Why it cannot be wrong |
|---|---|---|
| campaign | the campaign being generated | the loop is per campaign |
| line item | that campaign's line items, filtered to ones flighted on this day | comes from `ecosystem.line_item_nodes(campaign_id)` |
| advertiser | `campaign.advertiser_id` | read, never drawn |
| creative | that advertiser's servable creatives that existed on the day | `ecosystem.creative_sampler_as_of` |
| placement | inventory compatible with the creative's type and format | `ecosystem.placement_pool(creative_type, region)` |
| publisher | `placement.publisher` | read, never drawn |
| audience | segments in the publisher's market | `ecosystem.audience_sampler(country)` |

The creative → placement step is a genuine compatibility matrix
(`reference/business.yml`): a VIDEO creative only runs on pre/mid-roll or
interstitial inventory, an AUDIO creative only on inventory whose ad format is
`AUDIO_INSTREAM`, which in turn only exists on AUDIO publishers.

## 4. Hitting the configured volumes exactly, while still varying

Clicks are not sampled with a per-impression coin flip. They are *allocated*:

1. Each campaign draws a CTR multiplier: `lognormal_multiplier(sigma)` - a
   log-normal with mean exactly 1, so varying it does not move the global total.
2. The configured click total is split across campaigns with the
   largest-remainder method, weighted by `impressions x ctr_multiplier` and
   capped at each campaign's impression count
   (`distributions.allocate_counts_capped`).
3. Inside a campaign-day, exactly that many impressions are chosen with
   Efraimidis-Spirakis weighted sampling without replacement, weighted by
   creative quality, placement type, device, audience and viewability.

The same pattern allocates conversions across campaigns and then across that
campaign's clicks.

The result: `clicks` and `conversions` row counts equal the configured numbers
**exactly**, while per-campaign CTR still spans an order of magnitude. Step 3 is
also why creative and placement quality show up in the analytics - within a
campaign-day, the good ones win the clicks.

A note on the configured rates: the profiles in `config/scales.yml` imply a 3%
blended CTR and a 10% CVR, which is one to two orders of magnitude above real
programmatic display (~0.05-0.1% CTR). That is what the brief specifies, and the
shape of the data is unaffected - but it does compress the derived economics
(CPA lands around $2 rather than $20-50). Lower `events.clicks` and
`events.conversions` in the scale profile for realistic rates; nothing else needs
to change.

## 5. Where the skew comes from

| Property | Mechanism |
|---|---|
| Campaigns per advertiser (2 vs 5 vs 20+) | Pareto weights x the advertiser's spend tier |
| Line items per campaign | Pareto weights |
| Traffic per campaign | advertiser appetite x Pareto draw x budget |
| Traffic per publisher | Pareto draw on `traffic_weight`, inherited by its placements |
| CTR per campaign | log-normal multiplier x industry CTR index |
| CTR per creative | log-normal multiplier |
| CTR per placement/device/audience | index tables in `reference/business.yml` |
| CVR per campaign | log-normal multiplier x objective index (a CONVERSIONS campaign converts ~4x an AWARENESS one) |
| ROAS per campaign | log-normal around the configured median x the industry's ROAS index |
| Traffic per hour and weekday | diurnal and day-of-week weight curves |

## 6. Pricing and spend

Auction prices are **CPMs**. The cost of a single impression is
`clearing_price / 1000`.

```
bid_price      = line_item.target_cpm x placement quality x noise   (raised to the floor if below it)
clearing_price = floor_price + (bid_price - floor_price) x Beta(2, 3)     -- second price
```

so `floor_price <= clearing_price <= bid_price` always holds.

A line item's `bid_amount` is expressed in the unit its campaign is billed in,
derived from the same effective CPM:

```
CPM campaign: bid_amount = target_cpm
CPC campaign: bid_amount = (target_cpm / 1000) / expected_ctr
CPA campaign: bid_amount = (target_cpm / 1000) / (expected_ctr x expected_cvr)
```

This is what keeps total spend comparable across billing types instead of
differing by orders of magnitude, and it means a campaign with a high expected
CTR correctly bids a lower CPC for the same CPM.

Spend rows are then derived from real delivery:

| Billing type | Grain | `impression_id` |
|---|---|---|
| CPM | one row per (campaign, line item, publisher, hour), summing clearing prices | NULL |
| CPC | one row per click | the click's impression |
| CPA | one row per conversion | the conversion's impression |

The CPM roll-up is how real billing systems aggregate high-volume display spend,
and it keeps the spend table from growing to the size of the impression table.
Set `spend.cpm_rollup: per_impression` in `config/generation.yml` to emit one row
per impression instead.

Conversion value is anchored to what the campaign actually cost:
`value = (campaign spend / conversions) x target_roas x noise`. ROAS is therefore
a real, controllable metric rather than an unrelated random number.

Billed spend exceeds media cost in the run report, and that is correct rather
than a rounding artefact: CPC and CPA prices are anchored to the *bid* CPM, while
inventory is bought at the *clearing* price, which a second-price auction puts
below the bid. The difference is the platform's margin, exactly as it is for a
real DSP. CPM-billed campaigns have no such gap because the roll-up sums the
clearing prices directly.

## 7. Temporal model

The simulation clock is fixed (`timeline.simulation_end_date`) so that a seed
reproduces the same data on any day. "Now" is midnight at the start of that date.

```
advertiser.created_at
  <= campaign.created_at        (and strictly before the flight opens)
  <= line_item.created_at       (also before the flight opens)
  <= impression_timestamp       (inside the campaign and line item flights)
  <= click_timestamp            (log-normal delay, seconds to minutes)
  <= conversion_timestamp       (log-normal delay in hours, inside the attribution window)
  <= created_at of each event   (ingestion lag)
```

Four guarantees deserve a note because they were designed for, not lucked into:

* **A campaign can never predate its advertiser.** Flight dates are clamped to
  start at least two days after the advertiser was created. If the advertiser is
  too young for the requested status (a COMPLETED campaign for an account created
  last week) the status is downgraded to DRAFT rather than the dates being bent.
* **Status agrees with dates.** ACTIVE brackets "now", COMPLETED and CANCELLED
  ended before it, DRAFT has not started. Asserted in both validators.
* **Every delivery day has an eligible line item.** The first line item of each
  campaign always spans the entire flight.
* **An impression never uses a creative that did not exist yet.** Creatives are
  kept sorted by `created_at` and the eligible set is a binary-searched prefix
  (`creative_sampler_as_of`). Each advertiser's first creative is created within
  a day of the account, so the prefix is never empty.

Two deliberate simplifications, both documented because they are visible in the
data:

* Publishers, placements and audiences are onboarded **before** the event window
  opens. Filtering supply-side inventory per day would need a sampler per day per
  region; the alternative would be impressions on placements that did not exist.
* Click and conversion delays are truncated at the observation cutoff by
  redrawing uniformly inside the remaining time, rather than clamping. Clamping
  would pile events up on the cutoff instant and distort the last day of data.

Historical delivery is governed by **flight dates, not current status**: a
COMPLETED campaign served impressions while it was running, and a PAUSED campaign
served before it was paused. Only DRAFT entities have never served.

## 8. Determinism

Every random draw comes from a `RandomStream` whose seed is
`blake2b(master_seed || stream path)`. Streams are named by entity type and
index, for example `("impressions", campaign_id, day_ordinal)`.

Two properties follow:

* **Adding a stream never shifts existing ones.** A new generator feature cannot
  silently change previously published data.
* **A campaign-day can be regenerated in isolation**, which is what would make
  parallel generation for the LARGE profile safe without losing reproducibility.

UUIDs are drawn from the stream (`uuid.UUID(int=..., version=4)`), never from
`uuid.uuid4()`. Money is quantised to `Decimal` before it leaves the generator so
float formatting cannot vary between platforms.

`tests/test_determinism.py` asserts that two runs with the same seed produce a
byte-identical dataset, and that a different seed produces a different one.
