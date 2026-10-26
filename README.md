# Vilnius shadow signals

Finds Vilnius businesses that look busy on Google but quiet in official records, and ranks them as leads for a tax analyst, each with the reasons behind it. Covers restaurants & cafés, bars and car repair.

Design choices, limitations and ideas for a production version are in [DECISIONS.md](DECISIONS.md).

## Run it

You only need **[Docker Desktop](https://www.docker.com/products/docker-desktop/)**. Install it, open it, and wait until it shows *Engine running*.

1. Get the project: `git clone https://github.com/Rokasert/vilnius-shadow-signals.git`, or on GitHub click **Code → Download ZIP** and unzip it.
2. Open a terminal in the project folder (the one with `docker-compose.yml`).
   - Windows: right-click inside the folder → *Open in Terminal*.
   - macOS: Terminal, then `cd ` and drag the folder in.
3. Run:
   ```
   docker compose up --build
   ```
4. When the terminal says *You can now view your Streamlit app*, open **http://localhost:8501**.

Stop with `Ctrl+C`. Next time `docker compose up` is enough.

No keys, accounts or downloads from the data sources are needed: everything runs from the data included in the repo (see [Data in this repo](#data-in-this-repo)). The first start downloads about 2.5 GB of software and takes a few minutes.

**Without Docker:** install [uv](https://docs.astral.sh/uv/getting-started/installation/), then in the project folder run `uv run shadow-signals run` followed by `uv run streamlit run app/streamlit_app.py`.

**If it doesn't start:**
- *Cannot connect to the Docker daemon*: Docker Desktop isn't running yet.
- *Port 8501 is already in use*: another copy of the app is open; close it first.

## The app

- **Leads**:
  - where each number comes from and how recent it is;
  - how many companies could be checked in each category;
  - every checked company ranked, with its points and the rule behind its tier;
  - one company in detail: its Google places and how they were linked to it, insured staff by month, annual accounts.

  Hover over a column name for its meaning.
- **Data**: browse the bronze, silver and gold tables, or ask your own question in SQL.

## How it works

```
official open data ─┐
Google Places API ──┼─► bronze (raw files, one folder per run) ─► silver (clean, linked) ─► gold (leads)
company websites ───┘                                                           │
                                                          output/<month>/ CSV ◄─┴─► Streamlit app
```

| Source | What it gives |
|---|---|
| [get.data.gov.lt](https://get.data.gov.lt): company register, VMI, Statistics Lithuania, address register, Employment Service | companies, legal forms, addresses, annual accounts, VAT registration, tax debt, main activity, job ads |
| [atvira.sodra.lt](https://atvira.sodra.lt) | insured staff and average wage per company per month |
| Google Places API (Text Search) | Vilnius places, reviews, opening hours, website |
| the places' own websites | company codes printed on them (robots.txt respected) |

1. **Linking.** Each Google place is linked to the company that runs it. The strongest evidence is the company code on the place's own website; next comes the same name, with an activity that fits and a presence in Vilnius. Only high-confidence links are used: about 92% of them are right, measured on places whose website states the code.
2. **Scoring.** Every linked company is scored except non-profits. Owner-run companies (MB, IĮ) get 60 hours a week for their working owners. A company must be *busy*: its busiest place has at least the median reviews of all Google places in its category. It then collects points:

   | Signal | Points |
   |---|---|
   | Its insured staff (48 h a week each, the legal maximum with overtime) leave at least a fifth of its opening hours uncovered | 2, or 3 if over half |
   | Far more reviews per worker, or less revenue per review, than 90% of its UAB/AB peers | 1 |
   | Average wage below 80% of the minimum wage | 1 |
   | 2024 accounts not filed | 1 |
   | Not VAT-registered since 2024 | 1 |

3. **Tiers.**
   - **Review**: 2+ points.
   - **Priority**: a staffing gap and 3+ points, not owner-run, with a company link that doesn't rest on the name alone.

   Each company carries its reasons and caveats in plain sentences.
4. **Monthly comparison.** Each run saves its lead list, and the next run shows which leads are new, dropped or moved.

## Data in this repo

- **`data/sample/`** (14 MB): a real data collection from 23 October 2026, cut down to what the three categories need. `docker compose up` replays it, so the results match the real run exactly.
- **`output/2026-10/`**: the results of that run (`leads.csv`, `company_assessment.csv`, `category_summary.csv`, `lead_changes.csv`, and `run_info.json` listing the data runs used).
- **Google data:** the sample includes it so reviewers can run the project without a Google key. It is here for the review only and will be removed afterwards (see [DECISIONS.md](DECISIONS.md)).

## Monthly use with fresh data

```
docker compose run --rm pipeline shadow-signals run --mode live
docker compose up
```

- **The first command:** downloads the official data again (a few minutes) and reuses the saved Google data.
- **Refreshing Google too:** copy `.env.example` to `.env`, set `GOOGLE_MAPS_API_KEY`, and add `--google live` to the first command. It shows the planned number of calls and asks before making them. The three categories need about 200 calls, within Google's free 1,000 a month.

## Project layout

| Path | What's in it |
|---|---|
| `src/shadow_signals/` | Python: command line (`cli.py`), downloads (`ingest/`), lake and warehouse, export, replay sample |
| `transform/` | dbt: silver and gold models, tests, seeds (searches, activity codes, checked links) |
| `app/streamlit_app.py` | the app |
| `tests/` | Python tests |

Commands: `shadow-signals run` (everything), or the steps `ingest`, `load`, `transform` and `export`; `sql "<query>"` queries the warehouse.

## Checks

- `uv run pytest` runs the Python tests.
- `uv run shadow-signals transform` runs the dbt data tests: unique keys, valid values, that every lead has reasons and rests only on high-confidence links, and that points and tiers follow the rules.
- GitHub Actions runs lint, the tests and a full replay on every push.
