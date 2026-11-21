# Databricks notebook source
# MAGIC %md
# MAGIC # Shares pipeline – sample data generator
# MAGIC
# MAGIC Creates every **raw** source table read by the `Shares` Lakeflow pipeline (`src/Shares.sql`) and fills it with
# MAGIC consistent, airline-flavoured sample data, so the pipeline produces non-empty output.
# MAGIC
# MAGIC | Schema parameter (Shares.yml) | Default | Tables |
# MAGIC |---|---|---|
# MAGIC | `raw_main_schema` | `dbo` | reference data, fleet, flights, legs, stations, ancillaries, bookings, supplier invoices |
# MAGIC | `raw_branch_schema` | `dbr` | `pnr_passengers`, `ticket_sales`, `flight_schedule`, `office_currencies` |
# MAGIC | `raw_feature_schema` | `dbf` | `email_consents` |
# MAGIC
# MAGIC **Scenario built into the data**
# MAGIC * 2 historical journeys (≈ 120 / 200 days ago) with optional ancillary sales and supplier invoices → feed the margin calculation.
# MAGIC * 2 future journeys departing exactly `today + FromOffset` → picked up by `BND_Ancillary` / `BND_Ancillary_Bundled`.
# MAGIC * 1 future journey outside the `FromOffset..ToOffset` window → ranked, but filtered out of the BND outputs.
# MAGIC * Future PNRs of three kinds: *optional buyers* (3 bundled + 1 optional ancillary → `AncillaryOfferScoreRanking`),
# MAGIC   *bundled-only* (→ `AncillaryOfferScoreRankingBundled`) and *no ancillaries*.
# MAGIC
# MAGIC Every table is **overwritten** on each run. All dates are relative to today, so re-run it whenever you need fresh data.

# COMMAND ----------

dbutils.widgets.text("raw_catalog", "raw")
dbutils.widgets.text("raw_main_schema", "dbo")
dbutils.widgets.text("raw_branch_schema", "dbr")
dbutils.widgets.text("raw_feature_schema", "dbf")
dbutils.widgets.text("FromOffset", "90")
dbutils.widgets.text("ToOffset", "90")
dbutils.widgets.text("seed", "42")

CATALOG = dbutils.widgets.get("raw_catalog")
MAIN = dbutils.widgets.get("raw_main_schema")
BRANCH = dbutils.widgets.get("raw_branch_schema")
FEATURE = dbutils.widgets.get("raw_feature_schema")
FROM_OFFSET = int(dbutils.widgets.get("FromOffset"))
TO_OFFSET = int(dbutils.widgets.get("ToOffset"))
SEED = int(dbutils.widgets.get("seed"))

for s in (MAIN, BRANCH, FEATURE):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{s}`")

# COMMAND ----------

# DBTITLE 1,Helpers
import random
import string
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta

random.seed(SEED)
TODAY = date.today()
LOAD_TS = datetime.now()

DWH_COLS = [("dwh_current_flag", "BOOLEAN"), ("dwh_delete_flag", "BOOLEAN"), ("dwh_load_ts", "TIMESTAMP")]


def save(schema, table, cols, rows):
    """Write rows (list of dicts) to <catalog>.<schema>.<table> with the given (name, type) columns + DWH columns."""
    ddl = ", ".join(f"`{c}` {t}" for c, t in cols + DWH_COLS)
    data = [tuple(r.get(c) for c, _ in cols) + (True, False, LOAD_TS) for r in rows]
    df = spark.createDataFrame(data, ddl)
    full_name = f"`{CATALOG}`.`{schema}`.`{table}`"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    print(f"{full_name:<60} {len(rows):>6} rows")


def rand_guid():
    return str(uuid.UUID(int=random.getrandbits(128)))


def at(d, hh, mm=0):
    return datetime.combine(d, time(hh, mm))

# COMMAND ----------

# DBTITLE 1,Reference data (ref_categories, ref_codes, sys_ref_*)
# Category names must match the literals used in Shares.sql
REF_CATEGORIES = {1: "Cabin Classes", 2: "Meal Services", 3: "World Regions", 4: "Ancillary GL Accounts"}
ref_items = []


def add_ref(category_id, name):
    ref_items.append(dict(REF_CODE_ID=len(ref_items) + 1, REF_CODE_NAME=name, REF_CATEGORY_ID=category_id,
                          ACTIVE=True, REC_DELETED=False))
    return len(ref_items)


CABIN_CLASS = {n: add_ref(1, n) for n in ["Economy", "Premium Economy", "Business", "First"]}
MEAL_SERVICE = {n: add_ref(2, n) for n in ["Standard Meal Service", "Regional Cuisine", "Chef Signature Menu", "Light Snack Service"]}
WORLD_REGION = {n: add_ref(3, n) for n in ["Europe", "North America", "Asia", "Middle East"]}
GL_ACCOUNT = {n: add_ref(4, n) for n in ["Ancillary Optional", "Ancillary Bundled", "Ancillary Misc Costs"]}

save(MAIN, "ref_categories",
     [("REF_CATEGORY_ID", "INT"), ("REF_CATEGORY_NAME", "STRING")],
     [dict(REF_CATEGORY_ID=k, REF_CATEGORY_NAME=v) for k, v in REF_CATEGORIES.items()])
save(MAIN, "ref_codes",
     [("REF_CODE_ID", "INT"), ("REF_CODE_NAME", "STRING"), ("REF_CATEGORY_ID", "INT"), ("ACTIVE", "BOOLEAN"), ("REC_DELETED", "BOOLEAN")],
     ref_items)

SYS_CATEGORIES = {1: "Boarding Methods", 2: "Aircraft Types"}
SYS_ITEMS = [
    (1, 1, "Jet Bridge", "JB"), (2, 1, "Remote Stand (Bus)", "RS"), (3, 1, "Walk-out Stand", "WO"),
    (4, 2, "Airbus A321neo", "32Q"), (5, 2, "Boeing 787-9", "789"),
]
BOARDING_METHODS = [i[0] for i in SYS_ITEMS if i[1] == 1]

save(MAIN, "sys_ref_categories",
     [("SYS_REF_CATEGORY_ID", "INT"), ("SYS_REF_CATEGORY_NAME", "STRING")],
     [dict(SYS_REF_CATEGORY_ID=k, SYS_REF_CATEGORY_NAME=v) for k, v in SYS_CATEGORIES.items()])
save(MAIN, "sys_ref_codes",
     [("SYS_REF_ID", "INT"), ("SYS_REF_CATEGORY_ID", "INT"), ("SYS_REF_NAME", "STRING"), ("SYS_REF_DISPLAY_CODE", "STRING"),
      ("SYS_REF_ACTIVE", "BOOLEAN"), ("REC_DELETED", "BOOLEAN")],
     [dict(SYS_REF_ID=i, SYS_REF_CATEGORY_ID=c, SYS_REF_NAME=n, SYS_REF_DISPLAY_CODE=d, SYS_REF_ACTIVE=True, REC_DELETED=False)
      for i, c, n, d in SYS_ITEMS])

# COMMAND ----------

# DBTITLE 1,Geography (countries, stations, terminal_gates, currencies)
CURRENCIES = [  # id, code, name, usd rate
    (1, "USD", "US Dollar", 1.0), (2, "EUR", "Euro", 1.08), (3, "GBP", "Pound Sterling", 1.27), (4, "PLN", "Polish Zloty", 0.25),
    (5, "AED", "UAE Dirham", 0.27), (6, "SGD", "Singapore Dollar", 0.74), (7, "JPY", "Japanese Yen", 0.0067),
]
FX = {c[0]: c[3] for c in CURRENCIES}

COUNTRIES = [  # id, name, iso2, iso3, local name, region, currency_id
    (1, "United Kingdom", "GB", "GBR", "United Kingdom", "Europe", 3),
    (2, "France", "FR", "FRA", "France", "Europe", 2),
    (3, "Germany", "DE", "DEU", "Deutschland", "Europe", 2),
    (4, "Spain", "ES", "ESP", "España", "Europe", 2),
    (5, "Italy", "IT", "ITA", "Italia", "Europe", 2),
    (6, "Portugal", "PT", "PRT", "Portugal", "Europe", 2),
    (7, "Netherlands", "NL", "NLD", "Nederland", "Europe", 2),
    (8, "Poland", "PL", "POL", "Polska", "Europe", 4),
    (9, "United States", "US", "USA", "United States", "North America", 1),
    (10, "United Arab Emirates", "AE", "ARE", "Al Imarat", "Middle East", 5),
    (11, "Singapore", "SG", "SGP", "Singapura", "Asia", 6),
    (12, "Japan", "JP", "JPN", "Nippon", "Asia", 7),
]
COUNTRY_CCY = {c[0]: c[6] for c in COUNTRIES}

STATIONS = [  # id, city, iata, icao, country_id, timezone, terminal
    (1, "London", "LHR", "EGLL", 1, "Europe/London", "Terminal 5"),
    (2, "Paris", "CDG", "LFPG", 2, "Europe/Paris", "Terminal 2E"),
    (3, "Frankfurt", "FRA", "EDDF", 3, "Europe/Berlin", "Terminal 1"),
    (4, "Madrid", "MAD", "LEMD", 4, "Europe/Madrid", "Terminal 4"),
    (5, "Rome", "FCO", "LIRF", 5, "Europe/Rome", "Terminal 3"),
    (6, "Lisbon", "LIS", "LPPT", 6, "Europe/Lisbon", "Terminal 1"),
    (7, "Amsterdam", "AMS", "EHAM", 7, "Europe/Amsterdam", "Schiphol Plaza"),
    (8, "Warsaw", "WAW", "EPWA", 8, "Europe/Warsaw", "Terminal A"),
    (9, "New York", "JFK", "KJFK", 9, "America/New_York", "Terminal 4"),
    (10, "Dubai", "DXB", "OMDB", 10, "Asia/Dubai", "Terminal 3"),
    (11, "Singapore", "SIN", "WSSS", 11, "Asia/Singapore", "Terminal 1"),
    (12, "Tokyo", "NRT", "RJAA", 12, "Asia/Tokyo", "Terminal 1"),
]
STATION = {s[0]: s for s in STATIONS}

save(MAIN, "currencies",
     [("CURRENCY_ID", "INT"), ("CURRENCY_ISO_CODE", "STRING"), ("CURRENCY_NAME", "STRING")],
     [dict(CURRENCY_ID=i, CURRENCY_ISO_CODE=c, CURRENCY_NAME=n) for i, c, n, _ in CURRENCIES])
save(MAIN, "countries",
     [("COUNTRY_ID", "INT"), ("COUNTRY_NAME", "STRING"), ("COUNTRY_ISO_CODE", "STRING"), ("COUNTRY_ISO3_CODE", "STRING"),
      ("COUNTRY_LOCAL_NAME", "STRING"), ("WORLD_REGION_ID", "INT")],
     [dict(COUNTRY_ID=i, COUNTRY_NAME=n, COUNTRY_ISO_CODE=c2, COUNTRY_ISO3_CODE=c3, COUNTRY_LOCAL_NAME=ln,
           WORLD_REGION_ID=WORLD_REGION[r]) for i, n, c2, c3, ln, r, _ in COUNTRIES])
save(MAIN, "stations",
     [("STATION_ID", "INT"), ("STATION_NAME", "STRING"), ("STATION_IATA_CODE", "STRING"), ("STATION_ICAO_CODE", "STRING"),
      ("STATION_PHONETIC_NAME", "STRING"), ("STATION_ALIAS_NAME", "STRING"), ("STATION_ROUTING_NAME", "STRING"),
      ("DEFAULT_TERMINAL_NAME", "STRING"), ("STATION_TIMEZONE", "STRING"), ("IS_EN_ROUTE_POINT", "BOOLEAN"),
      ("COUNTRY_ID", "INT"), ("ACTIVE", "BOOLEAN")],
     [dict(STATION_ID=i, STATION_NAME=n, STATION_IATA_CODE=iata, STATION_ICAO_CODE=icao, STATION_PHONETIC_NAME=n.upper(),
           STATION_ALIAS_NAME=f"{n} {iata}", STATION_ROUTING_NAME=f"{n} ({iata})", DEFAULT_TERMINAL_NAME=term,
           STATION_TIMEZONE=tz, IS_EN_ROUTE_POINT=False, COUNTRY_ID=cid, ACTIVE=True)
      for i, n, iata, icao, cid, tz, term in STATIONS])

gates = []
for sid, _, iata, *_ in STATIONS:
    for g in random.sample(range(1, 40), 3):
        gates.append(dict(TERMINAL_GATE_ID=len(gates) + 1, TERMINAL_GATE_NAME=f"{iata} Gate {random.choice('ABCD')}{g}", STATION_ID=sid))
GATES_BY_STATION = defaultdict(list)
for g in gates:
    GATES_BY_STATION[g["STATION_ID"]].append(g["TERMINAL_GATE_ID"])

save(MAIN, "terminal_gates",
     [("TERMINAL_GATE_ID", "INT"), ("TERMINAL_GATE_NAME", "STRING"), ("STATION_ID", "INT")], gates)

# COMMAND ----------

# DBTITLE 1,Fleet and flights (fleet_units, flights, flight_legs, journey_days)
FLEET = [  # id, aircraft name, registration
    (1, "Skyward Explorer", "SP-LRA"), (2, "Skyward Voyager", "SP-LRB"), (3, "Skyward Navigator", "SP-LRC"),
]
save(MAIN, "fleet_units",
     [("FLEET_UNIT_ID", "INT"), ("FLEET_UNIT_NAME", "STRING"), ("FLEET_UNIT_CODE", "STRING"), ("PSS_CODE", "STRING"),
      ("WEB_BOOKING_CODE", "STRING"), ("REVENUE_ACCOUNTING_CODE", "STRING")],
     [dict(FLEET_UNIT_ID=i, FLEET_UNIT_NAME=n, FLEET_UNIT_CODE=reg, PSS_CODE=f"SX{i:02d}", WEB_BOOKING_CODE=f"WEB-{reg}",
           REVENUE_ACCOUNTING_CODE=f"RA{100 + i}") for i, n, reg in FLEET])

ROUTES = {  # station id per day of journey (10 days, first and last day = home base)
    "A": [1, 2, 5, 4, 6, 7, 3, 8, 10, 1],
    "B": [9, 1, 7, 3, 8, 10, 11, 12, 2, 9],
}
ROUTE_NAMES = {"A": "European Capitals Air Journey", "B": "Around the World in Ten Stops"}

window_start = TODAY + timedelta(days=FROM_OFFSET)
FLIGHTS = [
    dict(FLIGHT_ID=101, FLIGHT_NUMBER="SX1101", route="A", start=TODAY - timedelta(days=200), fleet=1, kind="historical"),
    dict(FLIGHT_ID=102, FLIGHT_NUMBER="SX1102", route="B", start=TODAY - timedelta(days=120), fleet=2, kind="historical"),
    dict(FLIGHT_ID=103, FLIGHT_NUMBER="SX2101", route="A", start=window_start, fleet=1, kind="future"),
    dict(FLIGHT_ID=104, FLIGHT_NUMBER="SX2102", route="B", start=window_start, fleet=2, kind="future"),
    dict(FLIGHT_ID=105, FLIGHT_NUMBER="SX2103", route="A", start=TODAY + timedelta(days=TO_OFFSET + 60), fleet=3, kind="future"),
]

flights, legs, journey_days = [], [], []
for f in FLIGHTS:
    flights.append(dict(
        FLIGHT_ID=f["FLIGHT_ID"], FLEET_UNIT_ID=f["fleet"], FLIGHT_NUMBER=f["FLIGHT_NUMBER"], SCHED_DEPARTURE_DATE=f["start"],
        ROUTE_MARKETING_NAME=ROUTE_NAMES[f["route"]], CHARTER_FLAG=False, DELETED=False, DISTRIBUTE_TO_WEB=True,
        DISTRIBUTE_TO_INTRANET=True, DISTRIBUTE_TO_PSS=True, ROUTE_THEME=random.choice(["Culture", "Gastronomy", "Discovery"]),
        FLIGHT_TYPE="Scheduled Journey", SCHEDULE_STATUS="Confirmed", ACTIVE=True))
    route = ROUTES[f["route"]]
    for day, sid in enumerate(route, start=1):
        leg_date = f["start"] + timedelta(days=day - 1)
        first, last = day == 1, day == len(route)
        eta = None if first else at(leg_date, random.choice([7, 8, 9]), random.choice([0, 15, 30, 45]))
        etd = None if last else at(leg_date, random.choice([18, 19, 20]), random.choice([0, 15, 30, 45]))
        legs.append(dict(
            FLIGHT_LEG_ID=f["FLIGHT_ID"] * 100 + day, FLIGHT_ID=f["FLIGHT_ID"], LEG_DATE=leg_date, DAY_OF_JOURNEY=day,
            AIRPORT_ID=sid, ETA_LOCAL=eta, ETD_LOCAL=etd,
            STA=eta and eta - timedelta(minutes=15), STD=etd and etd + timedelta(minutes=10),
            UTC_OFFSET_DIFF=random.choice([-1, 0, 1]), BOARDING_METHOD=random.choice(BOARDING_METHODS),
            GATE_ID=random.choice(GATES_BY_STATION[sid]), OPS_REMARKS=None, LAST_UPDATED_TS=LOAD_TS, DELETED=False,
            GUID=rand_guid(), TURNAROUND_STATION=first or last, LEG_SEQUENCE=day, NON_REVENUE_FLAG=False))
        journey_days.append(dict(
            FLIGHT_ID=f["FLIGHT_ID"], DAY_OF_JOURNEY=day, DELETED=False,
            MEAL_SERVICE_ID=random.choice(list(MEAL_SERVICE.values())), CABIN_CLASS_ID=random.choice(list(CABIN_CLASS.values()))))

save(MAIN, "flights",
     [("FLIGHT_ID", "INT"), ("FLEET_UNIT_ID", "INT"), ("FLIGHT_NUMBER", "STRING"), ("SCHED_DEPARTURE_DATE", "DATE"),
      ("ROUTE_MARKETING_NAME", "STRING"), ("CHARTER_FLAG", "BOOLEAN"), ("DELETED", "BOOLEAN"), ("DISTRIBUTE_TO_WEB", "BOOLEAN"),
      ("DISTRIBUTE_TO_INTRANET", "BOOLEAN"), ("DISTRIBUTE_TO_PSS", "BOOLEAN"), ("ROUTE_THEME", "STRING"), ("FLIGHT_TYPE", "STRING"),
      ("SCHEDULE_STATUS", "STRING"), ("ACTIVE", "BOOLEAN")],
     flights)
save(MAIN, "flight_legs",
     [("FLIGHT_LEG_ID", "INT"), ("FLIGHT_ID", "INT"), ("LEG_DATE", "DATE"), ("DAY_OF_JOURNEY", "INT"), ("AIRPORT_ID", "INT"),
      ("ETA_LOCAL", "TIMESTAMP"), ("ETD_LOCAL", "TIMESTAMP"), ("STA", "TIMESTAMP"), ("STD", "TIMESTAMP"), ("UTC_OFFSET_DIFF", "INT"),
      ("BOARDING_METHOD", "INT"), ("GATE_ID", "INT"), ("OPS_REMARKS", "STRING"), ("LAST_UPDATED_TS", "TIMESTAMP"),
      ("DELETED", "BOOLEAN"), ("GUID", "STRING"), ("TURNAROUND_STATION", "BOOLEAN"), ("LEG_SEQUENCE", "INT"),
      ("NON_REVENUE_FLAG", "BOOLEAN")],
     legs)
save(MAIN, "journey_days",
     [("FLIGHT_ID", "INT"), ("DAY_OF_JOURNEY", "INT"), ("DELETED", "BOOLEAN"), ("MEAL_SERVICE_ID", "INT"), ("CABIN_CLASS_ID", "INT")],
     journey_days)

# COMMAND ----------

# DBTITLE 1,Ancillary catalogue (service_items, service_item_accounting, ancillary_products, ancillary_prices)
OPTIONAL_PRODUCTS = [  # suffix, name, duration range (hrs)
    ("LNG", "Premium Lounge Access", (2.0, 4.0)),
    ("CTY", "Layover City Tour", (3.0, 5.0)),
    ("SPA", "Airport Spa & Wellness", (1.0, 2.0)),
    ("CHF", "Chauffeur Transfer & Fast Track", (1.0, 1.5)),
]
BUNDLED_PRODUCTS = [
    ("BAG", "Checked Bag 23kg", (0.5, 0.5)),
    ("SEA", "Standard Seat Selection", (0.5, 0.5)),
]

products, products_by_station = [], defaultdict(list)
for sid, city, iata, *_ in STATIONS:
    for bundled, catalogue in ((False, OPTIONAL_PRODUCTS), (True, BUNDLED_PRODUCTS)):
        for suffix, name, dur in catalogue:
            p = dict(SERVICE_ITEM_ID=len(products) + 1, RFISC_CODE=f"{iata}{suffix}", SERVICE_ITEM_NAME=f"{city} – {name}",
                     bundled=bundled, duration=dur, station=sid,
                     price=0.0 if bundled else float(random.randrange(45, 240, 5)))
            products.append(p)
            products_by_station[sid].append(p)

PRICE_SWITCH = TODAY - timedelta(days=30)  # older fare period (historical journeys) / current fare period
prices = []
for p in products:
    prices.append(dict(SERVICE_ITEM_ID=p["SERVICE_ITEM_ID"], BASE_RETAIL_PRICE=round(p["price"] * 0.95, 2),
                       FARE_EFFECTIVE_FROM=TODAY - timedelta(days=730), FARE_EFFECTIVE_TO=PRICE_SWITCH - timedelta(days=1)))
    prices.append(dict(SERVICE_ITEM_ID=p["SERVICE_ITEM_ID"], BASE_RETAIL_PRICE=p["price"],
                       FARE_EFFECTIVE_FROM=PRICE_SWITCH, FARE_EFFECTIVE_TO=TODAY + timedelta(days=730)))


def price_on(product, d):
    return product["price"] if d >= PRICE_SWITCH else round(product["price"] * 0.95, 2)


save(MAIN, "service_items",
     [("SERVICE_ITEM_ID", "INT"), ("RFISC_CODE", "STRING"), ("SERVICE_ITEM_NAME", "STRING")], products)
save(MAIN, "service_item_accounting",
     [("SERVICE_ITEM_ID", "INT"), ("GL_ACCOUNT_ID", "INT")],
     [dict(SERVICE_ITEM_ID=p["SERVICE_ITEM_ID"],
           GL_ACCOUNT_ID=GL_ACCOUNT["Ancillary Bundled" if p["bundled"] else "Ancillary Optional"]) for p in products])
save(MAIN, "ancillary_products",
     [("ANCILLARY_PRODUCT_ID", "INT"), ("SERVICE_ITEM_ID", "INT"), ("BUNDLED_IN_FARE", "BOOLEAN"), ("REC_DELETED", "BOOLEAN")],
     [dict(ANCILLARY_PRODUCT_ID=p["SERVICE_ITEM_ID"], SERVICE_ITEM_ID=p["SERVICE_ITEM_ID"], BUNDLED_IN_FARE=p["bundled"],
           REC_DELETED=False) for p in products])
save(MAIN, "ancillary_prices",
     [("SERVICE_ITEM_ID", "INT"), ("BASE_RETAIL_PRICE", "DOUBLE"), ("FARE_EFFECTIVE_FROM", "DATE"), ("FARE_EFFECTIVE_TO", "DATE")],
     prices)

# COMMAND ----------

# DBTITLE 1,Ancillary offers per journey day (ancillary_offers)
offers, offers_by_day = [], defaultdict(list)
for f in FLIGHTS:
    for day, sid in enumerate(ROUTES[f["route"]], start=1):
        leg_date = f["start"] + timedelta(days=day - 1)
        for p in products_by_station[sid]:
            o = dict(
                ANCILLARY_OFFER_ID=len(offers) + 1, ANCILLARY_PRODUCT_ID=p["SERVICE_ITEM_ID"], FLIGHT_ID=f["FLIGHT_ID"],
                STATION_ID=sid, LEG_DATE=leg_date, ANCILLARY_OFFER_CODE=f"{p['RFISC_CODE']}01",
                ANCILLARY_OFFER_NAME=p["SERVICE_ITEM_NAME"] + (" (bundled)" if p["bundled"] else ""),
                OFFER_STATUS_ID=0, ANCILLARY_CATEGORY_ID=12, SALE_ACTIVE=True,
                SERVICE_START_TIME=at(leg_date, random.randint(8, 16), random.choice([0, 15, 30, 45])),
                SERVICE_DURATION_HRS=round(random.uniform(*p["duration"]), 1),
                INVENTORY_CAPACITY=200 if p["bundled"] else 30, REC_DELETED=False,
                product=p, price=price_on(p, leg_date))
            offers.append(o)
            offers_by_day[(f["FLIGHT_ID"], day)].append(o)

save(MAIN, "ancillary_offers",
     [("ANCILLARY_OFFER_ID", "INT"), ("ANCILLARY_PRODUCT_ID", "INT"), ("FLIGHT_ID", "INT"), ("STATION_ID", "INT"),
      ("LEG_DATE", "DATE"), ("ANCILLARY_OFFER_CODE", "STRING"), ("ANCILLARY_OFFER_NAME", "STRING"), ("OFFER_STATUS_ID", "INT"),
      ("ANCILLARY_CATEGORY_ID", "INT"), ("SALE_ACTIVE", "BOOLEAN"), ("SERVICE_START_TIME", "TIMESTAMP"),
      ("SERVICE_DURATION_HRS", "DOUBLE"), ("INVENTORY_CAPACITY", "INT"), ("REC_DELETED", "BOOLEAN")],
     offers)

# COMMAND ----------

# DBTITLE 1,Passengers, bookings and ancillary sales (passengers, passenger_bookings, ancillary_sales)
FIRST_NAMES = ["Anna", "James", "Maria", "Piotr", "Sofia", "Lucas", "Emma", "Noah", "Olivia", "Mateo", "Yuki", "Omar",
               "Chloe", "Liam", "Isabel", "Hugo", "Lena", "Arjun", "Zofia", "Daniel"]
LAST_NAMES = ["Smith", "Kowalski", "Garcia", "Rossi", "Muller", "Dubois", "Silva", "Tanaka", "Haddad", "Novak",
              "Jansen", "Brown", "Lopez", "Wilson", "Nowak", "Costa", "Fischer", "Martin", "Lee", "Moreau"]
POS_OFFICES = [("LON", "GBP"), ("PAR", "EUR"), ("NYC", "USD"), ("DXB", "AED")]

persons, passenger_bookings_rows, manifest, pnrs = [], [], [], []
booked = defaultdict(int)
used_pnrs = set()


def new_pnr():
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=7))  # 7 chars: Shares.sql uses LEFT(ref, 7)
        if code not in used_pnrs:
            used_pnrs.add(code)
            return code


def sell(pax_id, flight_id, day, bundled):
    o = random.choice([o for o in offers_by_day[(flight_id, day)] if o["product"]["bundled"] == bundled])
    manifest.append(dict(ANCILLARY_OFFER_ID=o["ANCILLARY_OFFER_ID"], PASSENGER_ID=pax_id,
                         EMD_AMOUNT=0.0 if bundled else o["price"], WAITLISTED=False, REC_DELETED=False))
    booked[o["ANCILLARY_OFFER_ID"]] += 1


def create_pnr(flight, profile, pax_count):
    pnr = new_pnr()
    office, ccy = random.choice(POS_OFFICES)
    pnrs.append(dict(PNR=pnr, FLIGHT_NO=flight["FLIGHT_NUMBER"], POS_OFFICE=office, CURRENCY_CODE=ccy, profile=profile))
    last = random.choice(LAST_NAMES)
    middle_days = list(range(2, len(ROUTES[flight["route"]])))  # exclude first / last day of journey
    for seq in range(1, pax_count + 1):
        pax_id = len(persons) + 1
        first = random.choice(FIRST_NAMES)
        persons.append(dict(PASSENGER_ID=pax_id, FIRST_NAME=first, LAST_NAME=last, PNR=pnr, PAX_SEQ=seq,
                            EMAIL=f"{first}.{last}.{pax_id}@example.com".lower()))
        passenger_bookings_rows.append(dict(PASSENGER_ID=pax_id, FLIGHT_ID=flight["FLIGHT_ID"], PNR_PAX_REF=f"{pnr}{seq}", PAX_SEQ=seq))
        fid = flight["FLIGHT_ID"]
        if profile == "historical":
            for d in random.sample(middle_days, random.randint(1, 3)):
                sell(pax_id, fid, d, bundled=False)
            for d in random.sample(middle_days, 2):
                sell(pax_id, fid, d, bundled=True)
        elif profile == "optional_buyer":      # >2 bundled + 1 optional  -> AncillaryOfferScoreRanking
            days = random.sample(middle_days, 4)
            for d in days[:3]:
                sell(pax_id, fid, d, bundled=True)
            sell(pax_id, fid, days[3], bundled=False)
        elif profile == "bundled_only":        # only bundled ancillaries -> AncillaryOfferScoreRankingBundled
            for d in random.sample(middle_days, 2):
                sell(pax_id, fid, d, bundled=True)
        # profile "no_ancillary": booking only


for f in FLIGHTS:
    if f["kind"] == "historical":
        for _ in range(20):
            create_pnr(f, "historical", 2)
    else:
        for i in range(8):
            create_pnr(f, "optional_buyer", 1 if i < 2 else 2)
        for _ in range(6):
            create_pnr(f, "bundled_only", 2)
        for _ in range(3):
            create_pnr(f, "no_ancillary", 2)

save(MAIN, "passengers",
     [("PASSENGER_ID", "INT"), ("FIRST_NAME", "STRING"), ("LAST_NAME", "STRING")], persons)
save(MAIN, "passenger_bookings",
     [("PASSENGER_ID", "INT"), ("FLIGHT_ID", "INT"), ("PNR_PAX_REF", "STRING"), ("PAX_SEQ", "INT")], passenger_bookings_rows)
save(MAIN, "ancillary_sales",
     [("ANCILLARY_OFFER_ID", "INT"), ("PASSENGER_ID", "INT"), ("EMD_AMOUNT", "DOUBLE"), ("WAITLISTED", "BOOLEAN"),
      ("REC_DELETED", "BOOLEAN")],
     manifest)

# COMMAND ----------

# DBTITLE 1,Supplier invoices for flown ancillaries (suppliers, supplier_invoices, supplier_invoice_lines)
suppliers = [dict(SUPPLIER_ID=sid, SUPPLIER_NAME=f"{city} Ground Experience Ltd", STATION_ID=sid)
             for sid, city, *_ in STATIONS]
supplier_invoice_rows, supplier_invoice_line_rows = [], []
for o in offers:
    if o["product"]["bundled"] or booked[o["ANCILLARY_OFFER_ID"]] == 0 or o["LEG_DATE"] >= TODAY - timedelta(days=31):
        continue
    qty = booked[o["ANCILLARY_OFFER_ID"]]
    ccy = COUNTRY_CCY[STATION[o["STATION_ID"]][4]]
    unit_cost_local = round(o["price"] * random.uniform(0.40, 0.75) / FX[ccy], 2)
    doc_id = len(supplier_invoice_rows) + 1
    supplier_invoice_rows.append(dict(
        SUPPLIER_INVOICE_ID=doc_id, SUPPLIER_ID=o["STATION_ID"], CURRENCY_ID=ccy, FLIGHT_ID=o["FLIGHT_ID"],
        SERVICE_STATION_ID=o["STATION_ID"], SERVICE_DATE=at(o["LEG_DATE"], 0), FX_RATE_TO_USD=FX[ccy],
        INVOICE_STATUS_ID=random.choice([1, 30]), INVOICE_TYPE_ID=6))
    supplier_invoice_line_rows.append(dict(
        SUPPLIER_INVOICE_ID=doc_id, SERVICE_ITEM_ID=o["product"]["SERVICE_ITEM_ID"], UNIT_COST=unit_cost_local,
        QTY_BILLED=qty, QTY_FLOWN=max(qty - random.choice([0, 0, 0, 1]), 0), LINE_COST=round(unit_cost_local * qty, 2)))

save(MAIN, "suppliers", [("SUPPLIER_ID", "INT"), ("SUPPLIER_NAME", "STRING"), ("STATION_ID", "INT")], suppliers)
save(MAIN, "supplier_invoices",
     [("SUPPLIER_INVOICE_ID", "INT"), ("SUPPLIER_ID", "INT"), ("CURRENCY_ID", "INT"), ("FLIGHT_ID", "INT"),
      ("SERVICE_STATION_ID", "INT"), ("SERVICE_DATE", "TIMESTAMP"), ("FX_RATE_TO_USD", "DOUBLE"), ("INVOICE_STATUS_ID", "INT"),
      ("INVOICE_TYPE_ID", "INT")],
     supplier_invoice_rows)
save(MAIN, "supplier_invoice_lines",
     [("SUPPLIER_INVOICE_ID", "INT"), ("SERVICE_ITEM_ID", "INT"), ("UNIT_COST", "DOUBLE"), ("QTY_BILLED", "INT"),
      ("QTY_FLOWN", "INT"), ("LINE_COST", "DOUBLE")],
     supplier_invoice_line_rows)

# COMMAND ----------

# DBTITLE 1,Branch schema: reservation system (pnr_passengers, ticket_sales, flight_schedule, office_currencies)
save(BRANCH, "pnr_passengers",
     [("PNR", "STRING"), ("PAX_SEQ", "INT"), ("FIRST_NAME", "STRING"), ("LAST_NAME", "STRING"), ("email", "STRING")],
     [dict(PNR=p["PNR"], PAX_SEQ=p["PAX_SEQ"], FIRST_NAME=p["FIRST_NAME"], LAST_NAME=p["LAST_NAME"], email=p["EMAIL"])
      for p in persons])

for i, p in enumerate(pnrs):  # cancel ~1 in 10 tickets (status 'X' is excluded by the pipeline)
    p["TICKET_STATUS"] = "X" if i % 10 == 9 else "A"
save(BRANCH, "ticket_sales",
     [("PNR", "STRING"), ("FLIGHT_NO", "STRING"), ("POS_OFFICE", "STRING"), ("CURRENCY_CODE", "STRING"), ("TICKET_STATUS", "STRING")],
     pnrs)

save(BRANCH, "flight_schedule",
     [("FLIGHT_NO", "STRING"), ("DEPARTURE_DATE", "DATE"), ("ROUTE_NAME", "STRING")],
     [dict(FLIGHT_NO=f["FLIGHT_NUMBER"], DEPARTURE_DATE=f["start"], ROUTE_NAME=ROUTE_NAMES[f["route"]]) for f in FLIGHTS])

CCY_NAMES = {c[1]: c[2] for c in CURRENCIES}
save(BRANCH, "office_currencies",
     [("POS_OFFICE", "STRING"), ("CURRENCY_CODE", "STRING"), ("CURRENCY_NAME", "STRING")],
     [dict(POS_OFFICE=o, CURRENCY_CODE=c, CURRENCY_NAME=CCY_NAMES[c]) for o, c in POS_OFFICES])

# COMMAND ----------

# DBTITLE 1,Feature schema: marketing consent (email_consents)
emails = []
for p in persons:
    addr = p["EMAIL"].upper()
    if random.random() < 0.15:  # older consent record -> pipeline keeps the latest one per address
        emails.append(dict(EMAIL_ADDRESS=addr, OPT_OUT_DATE=date(1900, 1, 1)))
        emails.append(dict(EMAIL_ADDRESS=addr, OPT_OUT_DATE=TODAY - timedelta(days=random.randint(5, 300))))
    else:  # 1900-01-01 = never opted out (opted in)
        emails.append(dict(EMAIL_ADDRESS=addr, OPT_OUT_DATE=date(1900, 1, 1)))

save(FEATURE, "email_consents", [("EMAIL_ADDRESS", "STRING"), ("OPT_OUT_DATE", "DATE")], emails)

# COMMAND ----------

# DBTITLE 1,Summary
print(f"Journeys in BND window (DEPARTURE_DATE = {window_start}): "
      f"{[f['FLIGHT_NUMBER'] for f in FLIGHTS if TODAY + timedelta(days=FROM_OFFSET) <= f['start'] <= TODAY + timedelta(days=TO_OFFSET)]}")
print(f"PNRs by profile: { {k: sum(1 for p in pnrs if p['profile'] == k) for k in sorted({p['profile'] for p in pnrs})} }")
display(spark.sql(f"SHOW TABLES IN `{CATALOG}`.`{MAIN}`"))
