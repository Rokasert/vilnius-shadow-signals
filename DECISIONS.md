# Decisions

The goal was a lead list a VMI analyst can trust: few false accusations, every number traceable, and something that runs with one command. Where accuracy and coverage pulled in different directions, I chose accuracy.

## Key decisions

**1. Everything runs locally with one command.**
Docker Compose runs the pipeline and the app; DuckDB holds the warehouse; dbt holds the SQL transformations and their tests; Streamlit is the app. Anyone can run it without accounts, and the analyst can query the warehouse in SQL.
- The lake code works against cloud storage unchanged, so moving to the cloud is a deployment step, not a rewrite (see Improvements).
- *Rejected:*
  - A hosted web app with a cloud database: it needs logins and would put business data online.
  - A full workflow scheduler: too heavy for one monthly job.
  - A separate database server: unnecessary for a few hundred thousand rows.

**2. Raw first, then clean, then results (bronze → silver → gold).**
- **Bronze:** every download is stored unchanged, with a manifest (row counts, checksum, schema fingerprint), one folder per run.
- **Silver:** cleans each source and links Google places to companies.
- **Gold:** the scoring.
- Every lead keeps the IDs of the data runs it came from, and each month's lead list is kept in the lake, so months can be compared.

**3. Google data from the official Places API, not scraping.**
Scraping Google Maps directly or through a scraping service breaks Google's terms. The API sits behind an opt-in switch, a dry run, a confirmation and a call cap, which keeps the cost at $0: 423 requests for the first probe, against 1,000 free a month.

**4. Three categories, chosen from data: restaurants & cafés, bars, car repair.**
All seven candidates were probed first.
- *Restaurants:* the busiest Google signal (median 253 reviews) and few companies that look under-staffed, so anomalies stand out.
- *Car repair:* the best linking to companies (61% of places).
- *Bars:* a strong signal, late hours.
- *Rejected:*
  - Hair & beauty: about half of all salons look under-staffed for legal reasons (chair rental, self-employment), so the list would mostly flag honest businesses.
  - Driving schools: only 68 companies, about 5 reviews each.
  - Cafés alone: official data can't separate them from restaurants.

**5. Linking places to companies by measured evidence.**
Scores come from how often each kind of evidence was right on 453 places whose own website states the company code:
- the same name with a fitting activity and a Vilnius presence: about 95%;
- a registered address alone: 1–9%.

Results:
- High-confidence links are right 92% of the time on that test, and 29 of 30 times in a blind manual check.
- A place without strong evidence stays unlinked rather than guessed.
- *Rejected:* fuzzy name matching (26–36% right) and matching on address alone.

**6. Transparent points instead of a model.**
There are no confirmed cases to train on, so each signal is a plain rule with a stated threshold, and each lead lists its reasons in sentences.
- **Busy:** the company's busiest place has at least the median reviews of *all* Google places in its category, linked or not, so the bar doesn't depend on which companies could be linked.
- **Staffing gap:** insured staff can legally work 48 h a week each (the Labour Code maximum with overtime). A gap counts when at least a fifth of the opening hours have nobody declared to work.
- **Peers for "busier than its size":** the UAB/AB companies of the same category, the only ones whose staff counts are complete.
- Two signals that measure the same thing count once.

**7. Built-in caution.**
- Only high-confidence links count.
- Staff is the higher of the latest month and the 12-month average.
- **Owner-run companies (MB, IĮ)** are scored with 60 h a week allowed for working owners, and can't be *priority*, because the number of owners isn't public.
- **Non-profits** aren't scored, since volunteer work is legal there.
- A *priority* lead needs a company link that doesn't rest on the name alone. Reviewing the first list showed why: a busy restaurant had been linked to a near-dormant company with the same name, a perfect-looking false lead.
- Links an analyst has checked (`transform/seeds/match_corrections.csv`) override the matcher in every later run.

**8. Real Google data in the replay sample.**
It lets reviewers run the full project without a key or costs. Google's terms don't allow storing Places content beyond place IDs, so it is in this private repository for the review only and will be removed afterwards.

## Known limitations
- **Coverage:** only places linked to a company with high confidence can be checked: 20% of bars, 26% of restaurants and 61% of car repair places. Most trading names differ from legal names, few websites print the company code, and open data has no list of which company runs which venue.
- **Google search:** results are ranked by relevance and not exhaustive; the café search in particular returned few places.
- **Reviews:** these are lifetime totals, not this month's activity. Tourist venues collect more reviews per customer.
- **Staff:** Sodra counts people, not hours; part-time staff look like full-time. Staff employed by a sister company at the same address are invisible.
- **How recent the data is:**
  - Staff and wages come from Sodra's monthly open data, which runs two months behind: in October 2026 the latest month is August 2026.
  - Revenue comes from the annual accounts companies file with the Register of Legal Entities. The open data set has 2025 accounts for only 7 of the scored companies so far, so 2024 is the latest year that can be compared.
- **Owner-run companies:** the 60-hour allowance is an assumption. An MB with several working members can legitimately cover more.
- **Small groups and one month:** 41 bars make rough peer thresholds, and the history has a single month so far.

## Improvements for a production version

**Link more places to companies:**
- **A licensed feed from a business directory** such as rekvizitai.vz.lt. Directories list each company's brand names, venue addresses, websites and phone numbers. In the blind manual check, a directory listing was often the only public source naming a venue's operator, so this would link many more places than names and websites can.
  - Scraping a directory would break its terms, so this means buying or licensing the data.
  - Phone numbers would become a strong extra match, since Google lists a phone for most places.
- **Probabilistic record linkage:** a statistical model that learns from the data how much each kind of agreement (name, address, phone, activity) raises the odds that a place and a company are the same, and combines them into one match probability. It would replace the fixed evidence table, and it scales to the whole country. Splink is an open-source tool for this.
- **A growing set of analyst-confirmed links,** with matching precision tracked every month.

**More trusted data (VMI has access to most of it):**
- **Cash-register receipts (i.EKA) and VAT invoices (i.SAF):** declared turnover per day and per place, the strongest check against visible activity.
- **The licence register with venue addresses:** alcohol licences and hygiene permits (with the number of workplaces). Together with **GPAIS car-repair declarations**, these would link places by exact address.
- **Sodra per-employee records:** contract type and hours. This would remove the part-time blind spot.
- **The register's list of MB members and IĮ owners:** to replace the 60-hour assumption.
- **Past VMI and labour-inspectorate results:** to measure which signals really find violations, and later to train a model.
- **Digital-platform reports (DAC7):** booking and delivery platforms report their sellers' income.

**Better scoring:**
- Review growth between months instead of lifetime totals.
- Peer groups by district and size.
- Analysts' verdicts on leads fed back to recalibrate the points.

**Cloud deployment (e.g. Google Cloud):**
- **Running:** the same container as a monthly job on a schedule.
- **Storage and queries:** the lake in cloud storage, the warehouse in a cloud data warehouse.
- **Security:** keys in a secret manager; the app behind single sign-on so only analysts can open it.
- **Infrastructure and orchestration:** all of it defined as code (Terraform); a workflow scheduler only once there are many dependent pipelines.

**Scale to all of Lithuania:**
- Search by municipality and category, then refresh only the places that matter: leads and near-threshold companies.
- Full monthly Google searches for every business would cost roughly $600–1,100 a month; aggregated counts or a data licence are cheaper.

**Freshness:** each source on its own schedule (Sodra monthly, registers weekly, accounts yearly, Google monthly), refreshing only when a source has changed.

**Monitoring:**
- Alerts on failed runs and on row counts or schemas that drift from the previous run.
- A dashboard of data-test results and match precision.

**Analyst workflow:**
- Record each lead's outcome in the app.
- Access control and an audit log.
- A data-protection impact assessment before using personal or Google data at scale.
