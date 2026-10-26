"""Bulk CSV exports from the Lithuanian open data API (get.data.gov.lt, built on Spinta).

`<model>/:format/csv` streams a whole table in one response; Spinta query syntax
(`field="value"`, `field>=value`, `select(...)`) narrows it at the source."""

from dataclasses import dataclass
from urllib.parse import quote

from shadow_signals.context import RunContext
from shadow_signals.http_client import HttpClient
from shadow_signals.ingest.common import CHUNK_SIZE, finish_csv
from shadow_signals.lake import Manifest

EXTRACTOR_VERSION = "1"
VILNIUS_MUNICIPALITY_ID = "8e92c395-419b-4b57-8f0c-568358a80fb0"


@dataclass(frozen=True)
class SpintaDataset:
    source: str
    dataset: str
    model: str
    query: str = ""

    @property
    def key(self) -> str:
        return f"{self.source}/{self.dataset}"

    def url(self, base_url: str) -> str:
        url = f"{base_url.rstrip('/')}/datasets/gov/{self.model}/:format/csv"
        return f"{url}?{quote(self.query, safe='=&()<>,._-')}" if self.query else url


VACANCY_COLUMNS = (
    "darbo_vietos_id,statusas,ikelimo_data,prelim_darbo_uzmokestis,vid_darbo_uzmokestis,"
    "maks_darbo_uzmokestis,valiuta,profesijos_grupes_kodas,profesijos_grupes_pav,profesijos_kodas,"
    "profesijos_pareigybes_pav,kontrakto_tipas,galioja_nuo,galioja_iki,ar_aktuali_siandien,"
    "ar_uzpildyta,darbo_vietu_skaicius,darbo_vietos_adresas,darbo_vietos_sav_pav,"
    "registravimo_pagrindo_pav,jar_kodas,darbdavys,teisines_formos_pav,imones_iregistravimas,"
    "darbdavio_bustine"
)

DATASETS: list[SpintaDataset] = [
    # Registrų centras: legal entity register (JAR)
    SpintaDataset("jar", "legal_entities", "rc/jar/iregistruoti/JuridinisAsmuo"),
    SpintaDataset("jar", "registered_addresses", "rc/jar/buveines/Buveine"),
    SpintaDataset("jar", "legal_forms", "rc/jar/formos_statusai/Forma"),
    SpintaDataset("jar", "legal_statuses", "rc/jar/formos_statusai/Statusas"),
    SpintaDataset(
        "jar",
        "profit_loss_statements",
        "rc/jar/pelno_ataskaitos/PelnoAtaskaita",
        'laikotarpis_nuo>="2023-01-01"',
    ),
    SpintaDataset(
        "jar", "missing_financial_statements", "rc/jar/fa_dokumentu_nepateike/Nepateikes"
    ),
    SpintaDataset("jar", "late_financial_statements", "rc/jar/fa_veluojantys/VeluojantisPateikti"),
    # Registrų centras: address register, enough to turn a registered seat into street + number
    SpintaDataset("ar", "municipalities", "rc/ar/savivaldybe/Savivaldybe"),
    SpintaDataset("ar", "streets", "rc/ar/gatve/Gatve"),
    SpintaDataset(
        "ar",
        "buildings_vilnius",
        "rc/ar/pastatas/Pastatas",
        f'savivaldybe._id="{VILNIUS_MUNICIPALITY_ID}"',
    ),
    SpintaDataset("ar", "premises", "rc/ar/patalpa/Patalpa"),
    # State Tax Inspectorate (VMI)
    SpintaDataset("vmi", "taxpayers", "vmi/mm_registras/MokesciuMoketojas"),
    SpintaDataset(
        "vmi",
        "taxes_paid_vilnius",
        "vmi/ja_mokesciai/Moketojas",
        f'savivaldybe._id="{VILNIUS_MUNICIPALITY_ID}"&metai>=2024',
    ),
    SpintaDataset("vmi", "tax_arrears", "vmi/ja_nepriemokos/NepriemokosSuma"),
    SpintaDataset("vmi", "control_actions_by_sector", "vmi/kontroles_veiksmai/Veiksmas"),
    SpintaDataset("vmi", "audit_assessments_by_sector", "vmi/sprendimai/Sprendimas"),
    SpintaDataset(
        "vmi",
        "vat_returns_by_sector",
        "vmi/pvm_deklaracijos/VtsFr0600Mainai",
        'mokestinis_laikotarpis>="2024-01-01"',
    ),
    # Statistics Lithuania: activity codes for every legal entity, and the classifiers
    SpintaDataset("lsd", "legal_entity_activities", "lsd/cl/ja_asmenys/JuridinisAsmuo"),
    SpintaDataset("lsd", "evrk_2_1", "lsd/cl/evrk2_1/EkonominesVeiklosRusis"),
    SpintaDataset("lsd", "evrk_2", "lsd/cl/evrk/EkonominesVeiklosRusis"),
    # Employment Service: job vacancies in Vilnius, without contact persons or free text
    SpintaDataset(
        "uzt",
        "job_vacancies_vilnius",
        "uzt/ldv/Vieta",
        'darbo_vietos_sav_pav="Vilniaus miesto sav."&ikelimo_data>="2025-01-01"'
        f"&select({VACANCY_COLUMNS})",
    ),
]


def fetch(ctx: RunContext, http: HttpClient, ds: SpintaDataset) -> Manifest:
    url = ds.url(ctx.settings.spinta_base_url)
    with ctx.lake.bronze_run(ds.source, ds.dataset, ctx.snapshot_month, EXTRACTOR_VERSION) as run:
        run.manifest.request = {"url": url, "model": ds.model, "query": ds.query}
        run.manifest.format = {"type": "csv", "delimiter": ",", "compression": "gzip"}

        def land(response):
            return run.write_stream(
                "part-0001.csv.gz", response.iter_bytes(CHUNK_SIZE), compress=True
            )

        streamed = http.stream(url, land)
        run.manifest.http = {"status": streamed.status, "attempts": streamed.attempts}
        finish_csv(run, streamed.value, delimiter=",")
    return run.manifest


def extractors() -> dict:
    return {ds.key: (lambda ctx, http, ds=ds: fetch(ctx, http, ds)) for ds in DATASETS}
