# Databricks notebook source
# MAGIC %md
# MAGIC # Stats pipeline – sample data generator
# MAGIC
# MAGIC Creates every **source** table read by the `Stats` Lakeflow pipeline (`src/Stats.py` / `src/Stats.sql`) and fills it
# MAGIC with consistent, airline reservations contact-centre sample data, so that all seven output tables are non-empty.
# MAGIC
# MAGIC | Config key (Stats.yml) | Default | Tables |
# MAGIC |---|---|---|
# MAGIC | `gold_catalog`.`gold_schema` | `gold.gold` | `ContactSessions`, `ContactSessionMetrics`, `ContactParticipants`, `ContactSegments`, `ContactSkillAssignments`, `CampaignPhoneNumbers`, `AgentPrimaryPresence` |
# MAGIC | `raw_catalog`.`raw_main_schema` | `raw.main` | `agents`, `res_queues`, `agent_skills`, `business_units`, `call_outcome_codes`, `agent_presence_statuses` |
# MAGIC | `raw_catalog`.`raw_sup_schema` | `raw.sup` | `pnr_header`, `pnr_sales`, `pnr_agent_assignments`, `flights`, `res_agents`, `pnr_passengers`, `pnr_cancelled_passengers`, `pnr_cancellations` |
# MAGIC | `raw_catalog`.`raw_generic_schema` | `raw.generic` | `hr_staff` |
# MAGIC | `gold_catalog`.`gold_maindata_schema` | `gold.maindata` | `DimPassenger`, `DimGLAccount`, `DimFareProduct` |
# MAGIC | `gold_catalog`.`gold_ncf_schema` | `gold.ncf` | `NetFareRevenue` |
# MAGIC | `reporting_catalog`.`reporting_schema` | `reporting.reporting` | `PnrRevenueGrouped`, `PnrBookingStatus`, `dim_date`, `contact_res_agent_link` |
# MAGIC
# MAGIC **Scenario built into the data**
# MAGIC * ~20 reservation agents in Direct Sales, TA Sales, Service, Air Customer Service, Outbound and Chat, with 3 supervisors.
# MAGIC   One agent moved from Service to Direct Sales last month (SCD history), one agent left last month (→ `Termed` in `ResAgentsMonthly`).
# MAGIC * `days_of_calls` days of calls: answered, abandoned, short-abandoned, abandoned in IVR, outbound, voicemail, internal,
# MAGIC   callback, SMS and direct-extension calls, across all queues used by the `DeptGroup` / `Dept` rules.
# MAGIC * ~45 % of inbound calls dial a marketing **campaign number**; ~45 % of answered sales calls create a PNR *during the call*
# MAGIC   by the answering agent (→ `CampaignCalls.Order_Type = 'New'`), a quarter of those replace a PNR of the same frequent flyer
# MAGIC   cancelled the same day (→ `'Rebook'`).
# MAGIC * Agent presence history for every working day (→ `AgentPresenceByDay`).
# MAGIC
# MAGIC Every table is **overwritten** on each run. All dates are relative to today, so re-run it whenever you need fresh data.

# COMMAND ----------

dbutils.widgets.text("raw_catalog", "raw")
dbutils.widgets.text("raw_main_schema", "main")
dbutils.widgets.text("raw_sup_schema", "sup")
dbutils.widgets.text("raw_generic_schema", "generic")
dbutils.widgets.text("gold_catalog", "gold")
dbutils.widgets.text("gold_schema", "gold")
dbutils.widgets.text("gold_maindata_schema", "maindata")
dbutils.widgets.text("gold_ncf_schema", "ncf")
dbutils.widgets.text("reporting_catalog", "reporting")
dbutils.widgets.text("reporting_schema", "reporting")
dbutils.widgets.text("days_of_calls", "14")
dbutils.widgets.text("calls_per_day", "60")
dbutils.widgets.text("seed", "42")

W = dbutils.widgets.get
RAW_MAIN = (W("raw_catalog"), W("raw_main_schema"))
RAW_SUP = (W("raw_catalog"), W("raw_sup_schema"))
RAW_GENERIC = (W("raw_catalog"), W("raw_generic_schema"))
GOLD = (W("gold_catalog"), W("gold_schema"))
GOLD_MAINDATA = (W("gold_catalog"), W("gold_maindata_schema"))
GOLD_NCF = (W("gold_catalog"), W("gold_ncf_schema"))
REPORTING = (W("reporting_catalog"), W("reporting_schema"))
DAYS_OF_CALLS = int(W("days_of_calls"))
CALLS_PER_DAY = int(W("calls_per_day"))
SEED = int(W("seed"))

# naive datetimes below are UTC unless stated otherwise (pipeline sessions run in UTC as well)
spark.conf.set("spark.sql.session.timeZone", "UTC")

for catalog, schema in {RAW_MAIN, RAW_SUP, RAW_GENERIC, GOLD, GOLD_MAINDATA, GOLD_NCF, REPORTING}:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# DBTITLE 1,Helpers
import json
import random
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

random.seed(SEED)
LA = ZoneInfo("America/Los_Angeles")
UTC = ZoneInfo("UTC")
TODAY = date.today()
NOW = datetime.now(UTC).replace(tzinfo=None, microsecond=0)
HISTORY_START = datetime(2020, 1, 1)
HIGH_DATE = datetime(9999, 12, 31)

SCD_COLS = [("dwh_valid_from", "TIMESTAMP"), ("dwh_valid_to", "TIMESTAMP"),
            ("dwh_current_flag", "BOOLEAN"), ("dwh_delete_flag", "BOOLEAN")]
SCD_DEFAULTS = dict(dwh_valid_from=HISTORY_START, dwh_valid_to=HIGH_DATE, dwh_current_flag=True, dwh_delete_flag=False)


def save(target, table, cols, rows, scd=False):
    """Overwrite <catalog>.<schema>.<table> with rows (list of dicts); scd=True adds dwh_* SCD columns."""
    catalog, schema = target
    all_cols = cols + (SCD_COLS if scd else [])
    ddl = ", ".join(f"`{c}` {t}" for c, t in all_cols)
    as_utc = lambda v: v.replace(tzinfo=UTC) if isinstance(v, datetime) and v.tzinfo is None else v  # naive = UTC
    data = [tuple(as_utc(r.get(c, SCD_DEFAULTS.get(c) if scd else None)) for c, _ in all_cols) for r in rows]
    full_name = f"`{catalog}`.`{schema}`.`{table}`"
    spark.createDataFrame(data, ddl).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    print(f"{full_name:<62} {len(rows):>7} rows")


def guid():
    return str(uuid.UUID(int=random.getrandbits(128)))


def la_to_utc(local_naive):
    return local_naive.replace(tzinfo=LA).astimezone(UTC).replace(tzinfo=None)


def utc_to_la(utc_naive):
    return utc_naive.replace(tzinfo=UTC).astimezone(LA).replace(tzinfo=None)


def phone(prefix="1"):
    return prefix + str(random.choice([206, 213, 310, 415, 503, 604, 646, 702, 808, 949])) + f"{random.randint(0, 9999999):07d}"


def weighted(options):
    """options: dict value -> weight"""
    return random.choices(list(options), weights=list(options.values()))[0]


def first_of_month(d, months_back=0):
    y, m = d.year, d.month - months_back
    while m <= 0:
        y, m = y - 1, m + 12
    return date(y, m, 1)

# COMMAND ----------

# DBTITLE 1,Contact-centre reference data (business_units, res_queues, agent_skills, call_outcome_codes, agent_presence_statuses)
BUSINESS_UNITS = {name: guid() for name in ["Home", "US Inbound Queues", "UK", "AU"]}
save(RAW_MAIN, "business_units", [("id", "STRING"), ("name", "STRING")],
     [dict(id=i, name=n) for n, i in BUSINESS_UNITS.items()], scd=True)

# queue -> (business unit, description, agent department taking the calls)
QUEUES = {
    "Sales": ("US Inbound Queues", "New flight bookings - direct passengers", "Direct Sales"),
    "US - Internal Direct Sales - 2117": ("Home", "Internal transfer line - direct sales", "Direct Sales"),
    "Sales - TA": ("US Inbound Queues", "New flight bookings - travel agents", "TA Sales"),
    "Service": ("US Inbound Queues", "Changes to existing reservations", "Service"),
    "Round the World": ("Home", "Round-the-world fares and multi-city itineraries", "Service"),
    "Payment": ("Home", "Payments, refunds and fare differences", "Service"),
    "Charter Flights": ("Home", "Charter and group charter flights", "Service"),
    "Customer Relations": ("Home", "Complaints and compensation claims", "Service"),
    "Passenger Relations": ("Home", "Special assistance and passenger care", "Service"),
    "Air Customer Service": ("Home", "Schedule changes, seats, baggage", "Air Customer Service"),
    "Enroute": ("Home", "Passengers travelling today / disruption", "Air Customer Service"),
    "US - Air Resolutions": ("Home", "Irregular operations resolution desk", "Air Customer Service"),
    "Inside Sales": ("US Inbound Queues", "Premium cabin upsell", "Direct Sales"),
    "New Groups": ("US Inbound Queues", "Group bookings of 10+ passengers", "Direct Sales"),
    "Chat": ("US Inbound Queues", "Web chat and messaging", "Chat Sales"),
    "Operator": ("Home", "Switchboard", "Service"),
    "Outbound": ("US Inbound Queues", "Outbound sales campaigns", "Outbound"),
    "UK - Reservations": ("UK", "UK reservations line", "Direct Sales"),
}
QUEUE_IDS = {q: guid() for q in QUEUES}
queue_rows = [dict(id=QUEUE_IDS[q], name=q, description=d, businessUnitId=BUSINESS_UNITS[bu]) for q, (bu, d, _) in QUEUES.items()]
# SCD history: 'Sales' belonged to the 'Home' business unit before 2022-09-16
sales_old = dict(queue_rows[0], businessUnitId=BUSINESS_UNITS["Home"], dwh_valid_to=datetime(2022, 9, 16), dwh_current_flag=False)
queue_rows[0]["dwh_valid_from"] = datetime(2022, 9, 16)
save(RAW_MAIN, "res_queues",
     [("id", "STRING"), ("name", "STRING"), ("description", "STRING"), ("businessUnitId", "STRING")],
     queue_rows + [sales_old], scd=True)

SKILLS = {n: guid() for n in ["Consumer", "Travel Agent", "US - Round the World", "US - Irregular Ops", "Spanish", "Premium Cabin"]}
save(RAW_MAIN, "agent_skills", [("id", "STRING"), ("name", "STRING")],
     [dict(id=i, name=n) for n, i in SKILLS.items()], scd=True)

CALL_OUTCOMES = {n: guid() for n in ["New Booking", "Flight Change", "Seat Selection", "Baggage Enquiry", "Refund Request",
                                     "Upgrade Request", "Check-in Help", "Loyalty / Miles Enquiry", "Special Assistance"]}
save(RAW_MAIN, "call_outcome_codes", [("id", "STRING"), ("name", "STRING")],
     [dict(id=i, name=n) for n, i in CALL_OUTCOMES.items()], scd=True)

PRESENCES = {"ON_QUEUE": "On Queue", "AVAILABLE": "Available", "BUSY": "Busy", "BREAK": "Break", "MEAL": "Meal",
             "MEETING": "Meeting", "TRAINING": "Training", "AWAY": "Away", "OFFLINE": "Offline"}
PRESENCE_IDS = {code: guid() for code in PRESENCES}
save(RAW_MAIN, "agent_presence_statuses",
     [("id", "STRING"), ("systemPresence", "STRING"), ("languageLabels", "STRING")],
     [dict(id=PRESENCE_IDS[c], systemPresence=c, languageLabels=json.dumps({"en_US": label}))
      for c, label in PRESENCES.items()], scd=True)

# COMMAND ----------

# DBTITLE 1,Agents (agents, res_agents, contact_res_agent_link, hr_staff)
FIRST_NAMES = ["Olivia", "Liam", "Emma", "Noah", "Ava", "Mateo", "Sophia", "Lucas", "Mia", "Ethan", "Isla", "James",
               "Amelia", "Leo", "Harper", "Oscar", "Chloe", "Daniel", "Zoe", "Samuel", "Grace", "Henry", "Nora"]
LAST_NAMES = ["Bennett", "Clarke", "Turner", "Patel", "Nguyen", "Garcia", "Kowalski", "Murphy", "Okafor", "Silva",
              "Fischer", "Tanaka", "Rossi", "Hughes", "Moreau", "Larsen", "Kim", "Novak", "Reyes", "Walsh"]

# (department, title, how many)
TEAMS = [
    ("Direct Sales", "Travel Sales Consultant", 5),
    ("TA Sales", "Travel Agent Sales Consultant", 2),
    ("Service", "Passenger Service Consultant", 4),
    ("Air Customer Service", "Air Consultant", 3),
    ("Outbound", "Outbound Sales Consultant", 2),
    ("Chat Sales", "Web Chat Sales Consultant", 2),
]
HR_STATES = ["CA", "WA", "AZ", "NV", "TX", "OR"]

agents = []  # one dict per person; SCD versions are produced from it below
names = random.sample([(f, l) for f in FIRST_NAMES for l in LAST_NAMES], 30)


def new_agent(department, title, hire, supervisor=None, bu="Home"):
    first, last = names.pop()
    a = dict(id=guid(), first=first, last=last, name=f"{first} {last}", username=f"{first}.{last}".lower(),
             email=f"{first}.{last}@example-air.com".lower(), staffNumber=str(random.randint(100000, 199999)),
             hireDate=hire, department=department, title=title, businessUnitId=BUSINESS_UNITS[bu], businessUnitName=bu,
             supervisorId=supervisor["id"] if supervisor else None, state="active",
             locationId1=random.choice([None, "LAX-HQ", "SEA-REMOTE"]), locationId2="LAX-HQ",
             res_agent_id=len(agents) + 5001, hr_state=random.choice(HR_STATES))
    agents.append(a)
    return a


supervisors = [new_agent("Reservations", "Reservations Supervisor", date(2022, 3, 1) + timedelta(days=90 * i)) for i in range(3)]
for dept, title, n in TEAMS:
    for _ in range(n):
        hire = date(2023, 1, 9) + timedelta(days=random.randint(0, 900))
        hire = min(hire, TODAY - timedelta(days=DAYS_OF_CALLS + 45))
        new_agent(dept, title, hire, random.choice(supervisors), bu=random.choice(["Home", "Home", "US Inbound Queues"]))
uk_agent = new_agent("Direct Sales", "Travel Sales Consultant", date(2024, 2, 5), supervisors[0], bu="UK")

# scenario 1: an agent moved from Service to Direct Sales on the 15th of last month
mover = next(a for a in agents if a["department"] == "Service")
mover["move_date"] = datetime.combine(first_of_month(TODAY, 1) + timedelta(days=14), time(0))
# scenario 2: a Direct Sales agent left on the 10th of last month
leaver = next(a for a in agents if a["department"] == "Direct Sales" and a is not mover)
leaver["leave_date"] = first_of_month(TODAY, 1) + timedelta(days=9)
leaver["hireDate"] = min(leaver["hireDate"], leaver["leave_date"] - timedelta(days=200))

AGENT_COLS = [("id", "STRING"), ("name", "STRING"), ("username", "STRING"), ("email", "STRING"), ("staffNumber", "STRING"),
              ("hireDate", "DATE"), ("department", "STRING"), ("title", "STRING"), ("businessUnitId", "STRING"),
              ("businessUnitName", "STRING"), ("supervisorId", "STRING"), ("state", "STRING"),
              ("locationId1", "STRING"), ("locationId2", "STRING")]
agent_versions = []
for a in agents:
    start = datetime.combine(a["hireDate"], time(0))
    if "move_date" in a:
        agent_versions.append(dict(a, dwh_valid_from=start, dwh_valid_to=a["move_date"], dwh_current_flag=False))
        agent_versions.append(dict(a, department="Direct Sales", title="Travel Sales Consultant", dwh_valid_from=a["move_date"]))
    elif "leave_date" in a:
        left = datetime.combine(a["leave_date"], time(0))
        agent_versions.append(dict(a, dwh_valid_from=start, dwh_valid_to=left, dwh_current_flag=False))
        agent_versions.append(dict(a, state="inactive", dwh_valid_from=left))
    else:
        agent_versions.append(dict(a, dwh_valid_from=start))
save(RAW_MAIN, "agents", AGENT_COLS, agent_versions, scd=True)

# reservation-system (PSS) user accounts of the same people, linked by e-mail local part
save(RAW_SUP, "res_agents",
     [("agentId", "INT"), ("firstName", "STRING"), ("lastName", "STRING"), ("email", "STRING")],
     [dict(agentId=a["res_agent_id"], firstName=a["first"].upper(), lastName=a["last"].upper(),
           email=a["email"].upper() if i % 4 == 0 else a["email"]) for i, a in enumerate(agents)], scd=True)

save(REPORTING, "contact_res_agent_link",
     [("Contact_AgentID", "STRING")] + [(f"Res_AgentID{i}", "INT") for i in range(1, 9)],
     [dict(Contact_AgentID=a["id"], Res_AgentID1=a["res_agent_id"],
           Res_AgentID2=a["res_agent_id"] + 1000 if i % 5 == 0 else None) for i, a in enumerate(agents)])

hr_rows = []
for i, a in enumerate(agents):
    row = dict(staffNumber=a["staffNumber"], name=a["name"], workEmail=f" {a['first']}.{a['last']}@Example-Air.com ",
               state=a["hr_state"], leaveDate=a.get("leave_date"), dwh_load_date=datetime(2023, 1, 1))
    if i % 3 == 0:  # migrated from the legacy HR system -> validity starts at the load date
        row["dwh_valid_from"] = datetime(1900, 1, 1)
    else:
        row["dwh_valid_from"] = datetime.combine(first_of_month(a["hireDate"]), time(0))
    if i % 7 == 1:  # moved to another state: previous HR version
        changed = datetime.combine(first_of_month(TODAY, 4), time(0))
        hr_rows.append(dict(row, state="CA", dwh_valid_to=changed, dwh_current_flag=False))
        row["dwh_valid_from"] = changed
    hr_rows.append(row)
save(RAW_GENERIC, "hr_staff",
     [("staffNumber", "STRING"), ("name", "STRING"), ("workEmail", "STRING"), ("state", "STRING"), ("leaveDate", "DATE"),
      ("dwh_load_date", "TIMESTAMP")], hr_rows, scd=True)

# COMMAND ----------

# DBTITLE 1,Flights and campaign numbers (flights, CampaignPhoneNumbers)
ROUTES = [("XA101", "Los Angeles (LAX) - New York (JFK)"), ("XA115", "Los Angeles (LAX) - London (LHR)"),
          ("XA207", "San Francisco (SFO) - Honolulu (HNL)"), ("XA233", "Seattle (SEA) - Tokyo (NRT)"),
          ("XA310", "Los Angeles (LAX) - Sydney (SYD)"), ("XA412", "New York (JFK) - Paris (CDG)"),
          ("XA518", "Vancouver (YVR) - Los Angeles (LAX)"), ("XA620", "Los Angeles (LAX) - Cancun (CUN)"),
          ("XA777", "Round the World: LAX-LHR-SIN-SYD-LAX"), ("XC900", "Charter: Las Vegas (LAS) - Cabo (SJD)")]
save(RAW_SUP, "flights", [("FlightNumber", "STRING"), ("RouteName", "STRING"), ("DepartureDate", "DATE")],
     [dict(FlightNumber=f, RouteName=r, DepartureDate=TODAY + timedelta(days=random.randint(10, 200))) for f, r in ROUTES],
     scd=True)

CAMPAIGNS = {"18005550101": "Search - Brand", "18005550102": "Display - Winter Sun", "18005550103": "Email - Loyalty Offers",
             "18005550104": "Social - Flash Sale", "18005550105": "Affiliate - Metasearch", "18005550106": "TV - Holiday Season"}
INACTIVE_CAMPAIGN = "18005550199"
MAIN_LINES = ["18005550000", "18005550001"]  # reservations main line, travel-agent desk
save(GOLD, "CampaignPhoneNumbers", [("phone_number", "STRING"), ("campaign_name", "STRING"), ("active_flag", "INT")],
     [dict(phone_number=n, campaign_name=c, active_flag=1) for n, c in CAMPAIGNS.items()]
     + [dict(phone_number=INACTIVE_CAMPAIGN, campaign_name="Print - Inflight Magazine (ended)", active_flag=0)])

# COMMAND ----------

# DBTITLE 1,Calls (ContactSessions, ContactSessionMetrics, ContactParticipants, ContactSegments, ContactSkillAssignments)
sessions, metrics, participants, segments, skill_links = [], [], [], [], []
bookings = []  # PNRs created by an agent during a call, materialised in the next cell

QUEUE_WEIGHTS = {"Sales": 30, "Service": 18, "Sales - TA": 8, "Air Customer Service": 8, "Customer Relations": 5,
                 "Round the World": 3, "Payment": 3, "Charter Flights": 2, "Passenger Relations": 3, "Enroute": 3,
                 "US - Air Resolutions": 2, "Inside Sales": 2, "New Groups": 2, "Operator": 2,
                 "US - Internal Direct Sales - 2117": 2, "UK - Reservations": 1}
QUEUE_SKILLS = {"Sales": {"Consumer": 7, "Travel Agent": 2, "Spanish": 1}, "Sales - TA": {"Travel Agent": 1},
                "Service": {"Consumer": 6, "US - Irregular Ops": 2, "US - Round the World": 1, "Travel Agent": 1},
                "Round the World": {"US - Round the World": 1}}
SALES_QUEUES = {"Sales", "Sales - TA", "US - Internal Direct Sales - 2117", "Inside Sales", "UK - Reservations"}
CALL_TYPES = {"answered": 66, "abandoned": 8, "short_abandon": 4, "ivr_hangup": 2, "outbound": 8, "outbound_internal": 2,
              "voicemail": 1, "callback": 3, "sms": 4, "direct_ext": 2}
working_agents = {}
for a in agents:
    if "leave_date" not in a and a["department"] != "Reservations":
        working_agents.setdefault(a["department"], []).append(a)
working_agents["Direct Sales"] = [a for a in working_agents["Direct Sales"] if a is not uk_agent]


def agent_for(dept, at_utc):
    """Random agent of a department; the mover counts as Service before and Direct Sales after the move."""
    pool = [a for a in working_agents[dept] if a is not mover]
    if dept == ("Direct Sales" if at_utc >= mover["move_date"] else "Service"):
        pool.append(mover)
    return random.choice(pool)


def add_party(call, purpose, *, agent=None, direction="inbound", media="voice", ani=None, dnis=None, ring=1, bullseye=1):
    party, leg = guid(), guid()
    participants.append(dict(interactionId=call, partyId=party, purpose=purpose, agentId=agent["id"] if agent else None,
                             participantName=agent["name"] if agent else purpose.upper()))
    sessions.append(dict(interactionId=call, partyId=party, callLegId=leg, direction=direction, ani=ani, dnis=dnis,
                         routingRing=ring, agentBullseyeRing=bullseye,
                         usedRouting=random.choice(["Bullseye", "Standard", "Preferred"]) if purpose == "agent" else None,
                         mediaType=media, remote=bool(random.random() < 0.3) if agent else None))
    return party, leg


def add_metric(call, party, leg, at, name, value):
    metrics.append(dict(interactionId=call, partyId=party, callLegId=leg, eventTimestamp=at, name=name, value=float(value)))


def add_segment(call, party, leg, kind, start, end, queue=None, disconnect=None, outcome=None):
    segments.append(dict(interactionId=call, partyId=party, callLegId=leg, segmentType=kind, segmentStart=start, segmentEnd=end,
                         queueId=QUEUE_IDS.get(queue), disconnectType=disconnect, callOutcomeCode=CALL_OUTCOMES.get(outcome)))


def add_skills(call, party, leg, queue):
    if queue in QUEUE_SKILLS:
        for s in {weighted(QUEUE_SKILLS[queue]) for _ in range(random.choice([1, 1, 2]))}:
            skill_links.append(dict(interactionId=call, partyId=party, callLegId=leg, skillId=SKILLS[s]))


def agent_handles(call, agent, queue, start, *, ani, dnis, direction="inbound", media="voice", answered=True, wait_ms=0,
                  purpose="agent"):
    """Agent leg: alert -> interact (-> hold) -> wrap-up. Returns (answer, talk_end, end)."""
    ring = weighted({1: 8, 2: 2, 3: 1})
    party, leg = add_party(call, purpose, agent=agent, direction=direction, media=media, ani=ani, dnis=dnis,
                           ring=ring, bullseye=1 if ring == 1 else weighted({1: 1, 2: 2}))
    answer = start + timedelta(seconds=6)
    talk = random.randint(120, 900) if media != "message" else random.randint(300, 1500)
    hold = random.choice([0, 0, 0, random.randint(20, 180)])
    acw = random.randint(15, 120)
    talk_end = answer + timedelta(seconds=talk + hold)
    end = talk_end + timedelta(seconds=acw)
    outcome = "New Booking" if queue in SALES_QUEUES else random.choice(list(CALL_OUTCOMES))
    add_segment(call, party, leg, "alert", start, answer, queue)
    add_segment(call, party, leg, "interact", answer, talk_end, queue, disconnect=random.choice(["client", "peer", "endpoint"]))
    add_segment(call, party, leg, "wrapup", talk_end, end, queue, outcome=outcome)
    add_skills(call, party, leg, queue)
    if answered:
        add_metric(call, party, leg, answer, "tAnswered", wait_ms + 6000)
        if wait_ms > 20000:
            add_metric(call, party, leg, answer, "nOverSla", 1)
    add_metric(call, party, leg, talk_end, "tTalkComplete", talk * 1000)
    if hold:
        add_metric(call, party, leg, talk_end, "tHeldComplete", hold * 1000)
    add_metric(call, party, leg, end, "tAcw", acw * 1000)
    add_metric(call, party, leg, end, "tHandle", (talk + hold + acw) * 1000)
    if random.random() < 0.05:
        add_metric(call, party, leg, end, "nTransferred", 1)
    return answer, talk_end, end


def inbound_call(day, kind):
    call = guid()
    start = la_to_utc(datetime.combine(day, time(7)) + timedelta(seconds=random.randint(0, 10 * 3600)))
    queue = weighted(QUEUE_WEIGHTS)
    dnis_number = random.choice(list(CAMPAIGNS)) if random.random() < 0.45 else random.choice(MAIN_LINES + [INACTIVE_CAMPAIGN])
    if kind == "ivr_hangup":
        dnis_number = random.choice(list(CAMPAIGNS))
    ani, dnis = "tel:+" + phone(), "tel:+" + dnis_number
    add_party(call, "customer", ani=ani, dnis=dnis)

    if kind == "ivr_hangup":  # caller hung up in the IVR before reaching a queue
        party, leg = add_party(call, "ivr", ani=ani, dnis=dnis)
        end = start + timedelta(seconds=random.randint(20, 90))
        add_segment(call, party, leg, "ivr", start, end, disconnect="peer")
        add_metric(call, party, leg, end, "tIvr", (end - start).total_seconds() * 1000)
        return

    offer = start + timedelta(seconds=random.randint(30, 90))  # time spent in the IVR
    acd_party, acd_leg = add_party(call, "acd", ani=ani, dnis=dnis)
    add_skills(call, acd_party, acd_leg, queue)
    add_metric(call, acd_party, acd_leg, offer, "nOffered", 1)

    if kind in ("abandoned", "short_abandon"):
        wait = random.randint(2, 9) if kind == "short_abandon" else random.randint(15, 600)
        gone = offer + timedelta(seconds=wait)
        add_segment(call, acd_party, acd_leg, "interact", offer, gone, queue, disconnect="peer")
        add_metric(call, acd_party, acd_leg, gone, "tAbandon", wait * 1000)
        if kind == "short_abandon":
            add_metric(call, acd_party, acd_leg, gone, "tShortAbandon", wait * 1000)
        return

    wait = random.randint(5, 240)
    connected = offer + timedelta(seconds=wait)
    add_segment(call, acd_party, acd_leg, "interact", offer, connected, queue)
    add_metric(call, acd_party, acd_leg, connected, "tAcd", wait * 1000)
    agent = agent_for(QUEUES[queue][2], connected)
    if queue == "UK - Reservations":
        agent = uk_agent
    answer, talk_end, end = agent_handles(call, agent, queue, connected, ani=ani, dnis=dnis, wait_ms=wait * 1000)

    if queue in SALES_QUEUES and random.random() < 0.45:  # PNR created by the agent while on the call
        booked_at = answer + (talk_end - answer) * random.uniform(0.4, 0.9)
        bookings.append(dict(agent=agent, created_utc=booked_at, queue=queue, rebook=random.random() < 0.25))


def outbound_call(day, kind):
    call = guid()
    start = la_to_utc(datetime.combine(day, time(8)) + timedelta(seconds=random.randint(0, 9 * 3600)))
    company_line = "tel:+" + MAIN_LINES[0]
    if kind == "callback":  # scheduled callback from the Sales queue
        agent = agent_for("Direct Sales", start)
        agent_handles(call, agent, "Sales", start, ani=company_line, dnis="tel:+" + phone(), direction="outbound",
                      media="callback", wait_ms=random.randint(1000, 4000))
        return
    if kind == "sms":
        agent = agent_for("Chat Sales", start)
        direction = random.choice(["inbound", "outbound"])
        agent_handles(call, agent, "Chat", start, ani="tel:+" + phone(), dnis=company_line, direction=direction,
                      media="message", answered=direction == "inbound", wait_ms=random.randint(1000, 30000))
        return
    if kind == "direct_ext":  # passenger dialled an agent's direct extension
        agent = agent_for("Service", start)
        agent_handles(call, agent, None, start, ani="tel:+" + phone(), dnis="tel:+" + MAIN_LINES[0][:-4] + "4521",
                      purpose="user", answered=False)
        return
    agent = agent_for("Outbound", start)
    target = {"outbound": "tel:+" + phone(), "outbound_internal": str(random.randint(4000, 4999)), "voicemail": "*86"}[kind]
    agent_handles(call, agent, "Outbound" if kind == "outbound" else None, start, ani=company_line, dnis=target,
                  direction="outbound", answered=False)


call_days = [TODAY - timedelta(days=i) for i in range(DAYS_OF_CALLS, 0, -1)]
for day in call_days:
    for _ in range(CALLS_PER_DAY):
        kind = weighted(CALL_TYPES)
        if kind in ("answered", "abandoned", "short_abandon", "ivr_hangup"):
            inbound_call(day, kind)
        else:
            outbound_call(day, kind)

save(GOLD, "ContactSessions",
     [("interactionId", "STRING"), ("partyId", "STRING"), ("callLegId", "STRING"), ("direction", "STRING"), ("ani", "STRING"),
      ("dnis", "STRING"), ("routingRing", "INT"), ("agentBullseyeRing", "INT"), ("usedRouting", "STRING"),
      ("mediaType", "STRING"), ("remote", "BOOLEAN")], sessions)
save(GOLD, "ContactSessionMetrics",
     [("interactionId", "STRING"), ("partyId", "STRING"), ("callLegId", "STRING"), ("eventTimestamp", "TIMESTAMP"),
      ("name", "STRING"), ("value", "DOUBLE")], metrics)
save(GOLD, "ContactParticipants",
     [("interactionId", "STRING"), ("partyId", "STRING"), ("purpose", "STRING"), ("agentId", "STRING"),
      ("participantName", "STRING")], participants)
save(GOLD, "ContactSegments",
     [("interactionId", "STRING"), ("partyId", "STRING"), ("callLegId", "STRING"), ("segmentType", "STRING"),
      ("segmentStart", "TIMESTAMP"), ("segmentEnd", "TIMESTAMP"), ("queueId", "STRING"), ("disconnectType", "STRING"),
      ("callOutcomeCode", "STRING")], segments)
save(GOLD, "ContactSkillAssignments",
     [("interactionId", "STRING"), ("partyId", "STRING"), ("callLegId", "STRING"), ("skillId", "STRING")], skill_links)

# COMMAND ----------

# DBTITLE 1,Reservations (pnr_header, pnr_sales, pnr_agent_assignments, pnr_passengers, pnr_cancelled_passengers, pnr_cancellations)
pnr_header, pnr_sales, assignments, pax, cancelled_pax, cancellations = [], [], [], [], [], []
pnrs = []  # (pnr, status_text, gross_usd, currency, created_local, cancelled)
next_pnr = {"old": 6000001, "new": 7000001}
FX = {"USD": 1.0, "CAD": 1.37, "GBP": 0.79}


def create_pnr(created_local, agent=None, ffp=None, era="new", cancelled_on=None, ta=False, currency=None):
    pnr = next_pnr[era]
    next_pnr[era] += 1
    flight = random.choice(ROUTES)[0]
    currency = currency or ("CAD" if random.random() < 0.15 else "USD")
    fare = round(random.uniform(180, 2400), 2)
    stage = "CANCELLED" if cancelled_on else weighted({"OPTION": 2, "CONFIRMED": 5, "FINAL": 3})
    deposit, balance = round(fare * 0.2, 2), round(fare * 0.8, 2)
    received = {"OPTION": 0.0, "CONFIRMED": deposit, "FINAL": fare, "CANCELLED": deposit}[stage]
    pnr_header.append(dict(pnr=pnr, pnrCreatedDate=created_local))
    pnr_sales.append(dict(pnr=pnr, office="YVR" if currency == "CAD" else "LAX", DepositAmt=deposit, BalanceAmt=balance,
                          AgencyCommission=round(fare * 0.08, 2) if ta else 0.0, CurrencyCode=currency,
                          PromoCode=random.choice([None, None, "WINTERSUN", "FLASH20"]),
                          Status="K" if cancelled_on else "", ConfirmFlag="Y" if stage == "FINAL" else "N",
                          ReceivedAmt=received, BookingChannel="Phone" if agent else "Web", FlightNumber=flight,
                          DirectFlag="T" if ta else "D", CancelDate=cancelled_on))
    if agent:
        assignments.append(dict(pnr=pnr, assignedAgentId=agent["res_agent_id"]))
    people = [(random.choice(FIRST_NAMES), random.choice(LAST_NAMES)) for _ in range(weighted({1: 4, 2: 4, 3: 1, 4: 1}))]
    for n, (first, last) in enumerate(people, start=1):
        row = dict(pnr=pnr, PAXNUM=n, FNAME=first.upper(), LNAME=last.upper(), FlightNumber=flight,
                   ffpNumber=(ffp or f"XA{random.randint(100000000, 999999999)}") if n == 1 else None)
        (cancelled_pax if cancelled_on else pax).append(row)
    if cancelled_on:
        cancellations.append(dict(pnr=pnr, REASON=random.choice(["Schedule change", "Change of plans", "Rebooked to another flight"])))
    pnrs.append(dict(pnr=pnr, status=stage, gross=fare * len(people), currency=currency, created=created_local,
                     cancelled=bool(cancelled_on), pax=len(people)))
    return pnr


for b in sorted(bookings, key=lambda x: x["created_utc"]):
    created_local = utc_to_la(b["created_utc"]).replace(microsecond=0)  # PSS stores local (Pacific) time
    ffp = f"XA{random.randint(100000000, 999999999)}"
    if b["rebook"]:  # same frequent flyer cancelled an older PNR earlier the same day
        old_created = created_local - timedelta(days=random.randint(20, 120))
        create_pnr(old_created, ffp=ffp, era="old", cancelled_on=created_local - timedelta(minutes=random.randint(5, 60)))
    create_pnr(created_local, agent=b["agent"], ffp=ffp, ta=b["queue"] == "Sales - TA")

for day in call_days:  # web bookings, not linked to any call
    for _ in range(5):
        create_pnr(datetime.combine(day, time(0)) + timedelta(seconds=random.randint(0, 86399)))

save(RAW_SUP, "pnr_header", [("pnr", "BIGINT"), ("pnrCreatedDate", "TIMESTAMP")], pnr_header, scd=True)
save(RAW_SUP, "pnr_sales",
     [("pnr", "BIGINT"), ("office", "STRING"), ("DepositAmt", "DOUBLE"), ("BalanceAmt", "DOUBLE"),
      ("AgencyCommission", "DOUBLE"), ("CurrencyCode", "STRING"), ("PromoCode", "STRING"), ("Status", "STRING"),
      ("ConfirmFlag", "STRING"), ("ReceivedAmt", "DOUBLE"), ("BookingChannel", "STRING"), ("FlightNumber", "STRING"),
      ("DirectFlag", "STRING"), ("CancelDate", "TIMESTAMP")], pnr_sales, scd=True)
save(RAW_SUP, "pnr_agent_assignments", [("pnr", "BIGINT"), ("assignedAgentId", "INT")], assignments, scd=True)
PAX_COLS = [("pnr", "BIGINT"), ("PAXNUM", "INT"), ("FNAME", "STRING"), ("LNAME", "STRING"), ("ffpNumber", "STRING"),
            ("FlightNumber", "STRING")]
save(RAW_SUP, "pnr_passengers", PAX_COLS, pax, scd=True)
save(RAW_SUP, "pnr_cancelled_passengers", PAX_COLS, cancelled_pax, scd=True)
save(RAW_SUP, "pnr_cancellations", [("pnr", "BIGINT"), ("REASON", "STRING")], cancellations, scd=True)

# COMMAND ----------

# DBTITLE 1,Net fare revenue (NetFareRevenue, DimPassenger, DimGLAccount, DimFareProduct)
# AccountKey -> (fpaAccountGroup, AccountGroup, AccountType)
GL_ACCOUNTS = {1: ("Gross", "Revenue", "Base fare"), 2: ("Gross", "Revenue", "Carrier surcharge"),
               3: ("Discount", "Discount", "Promo discount"), 4: ("Voucher", "Voucher", "Travel voucher"),
               5: ("Gross", "Revenue", "Inflight gifts"), 6: ("Cost", "Cost", "Distribution cost"),
               7: ("Voucher redemption", "Voucher redemption", "Voucher redemption")}
# ProductKey -> (ProductCategory, IsGiftExpensed)
FARE_PRODUCTS = {1: ("Fare", False), 2: ("Ancillary", False), 3: ("Ancillary", True), 4: ("CancelRevenue", False)}

dim_pax, revenue = [], []
for p in pnrs:
    for n in range(1, p["pax"] + 1):
        key = p["pnr"] * 10 + n
        dim_pax.append(dict(JourneyPaxKey=key, TravelPnr=p["pnr"], BookingDateTime=p["created"], IsPaxCancelled=int(p["cancelled"])))
        fare = p["gross"] / p["pax"]
        lines = [(1, 1, fare * 0.85), (2, 1, fare * 0.15), (6, 1, -fare * 0.04)]
        if random.random() < 0.3:
            lines.append((3, 1, -round(fare * random.choice([0.05, 0.1, 0.2]), 2)))
        if random.random() < 0.1:
            lines.append((4, 1, -50.0))
        if random.random() < 0.2:
            lines.append((5, 3, 35.0))
        if p["cancelled"]:
            lines.append((1, 4, fare * 0.1))
        for account, product, usd in lines:
            usd = round(usd, 2)
            revenue.append(dict(JourneyPaxKey=key, AccountKey=account, ProductKey=product, AmountUSD=usd,
                                AmountLocalCcy=round(usd * FX[p["currency"]], 2)))

save(GOLD_MAINDATA, "DimPassenger",
     [("JourneyPaxKey", "BIGINT"), ("TravelPnr", "BIGINT"), ("BookingDateTime", "TIMESTAMP"), ("IsPaxCancelled", "INT")], dim_pax)
save(GOLD_MAINDATA, "DimGLAccount",
     [("AccountKey", "INT"), ("fpaAccountGroup", "STRING"), ("AccountGroup", "STRING"), ("AccountType", "STRING")],
     [dict(AccountKey=k, fpaAccountGroup=f, AccountGroup=g, AccountType=t) for k, (f, g, t) in GL_ACCOUNTS.items()])
save(GOLD_MAINDATA, "DimFareProduct", [("ProductKey", "INT"), ("ProductCategory", "STRING"), ("IsGiftExpensed", "BOOLEAN")],
     [dict(ProductKey=k, ProductCategory=c, IsGiftExpensed=g) for k, (c, g) in FARE_PRODUCTS.items()])
save(GOLD_NCF, "NetFareRevenue",
     [("JourneyPaxKey", "BIGINT"), ("AccountKey", "INT"), ("ProductKey", "INT"), ("AmountUSD", "DECIMAL(18,2)"),
      ("AmountLocalCcy", "DECIMAL(18,2)")],
     [dict(r, AmountUSD=Decimal(str(r["AmountUSD"])), AmountLocalCcy=Decimal(str(r["AmountLocalCcy"]))) for r in revenue])

# COMMAND ----------

# DBTITLE 1,Reporting tables (PnrRevenueGrouped, PnrBookingStatus, dim_date)
save(REPORTING, "PnrRevenueGrouped", [("pnr", "BIGINT"), ("GrossFareAmt", "DOUBLE")],
     [dict(pnr=p["pnr"], GrossFareAmt=round(p["gross"], 2)) for p in pnrs])
save(REPORTING, "PnrBookingStatus", [("pnr", "BIGINT"), ("StatusText", "STRING")],
     [dict(pnr=p["pnr"], StatusText=p["status"]) for p in pnrs])

calendar_start = date(TODAY.year - 1, 1, 1)
save(REPORTING, "dim_date",
     [("Date", "DATE"), ("Year", "INT"), ("MonthNumber", "INT"), ("MonthName", "STRING"), ("DayOfWeek", "STRING"),
      ("IsWeekend", "BOOLEAN")],
     [dict(Date=d, Year=d.year, MonthNumber=d.month, MonthName=d.strftime("%B"), DayOfWeek=d.strftime("%A"),
           IsWeekend=d.weekday() >= 5)
      for d in (calendar_start + timedelta(days=i) for i in range((TODAY - calendar_start).days + 120))])

# COMMAND ----------

# DBTITLE 1,Agent presence (AgentPrimaryPresence)
SHIFT = [("ON_QUEUE", 110), ("BREAK", 15), ("ON_QUEUE", 95), ("MEAL", 30), ("ON_QUEUE", 100), ("MEETING", 30),
         ("TRAINING", 30), ("ON_QUEUE", 80), ("AWAY", 10), ("ON_QUEUE", 60)]
presence = []
for a in agents:
    if "leave_date" in a:
        continue
    for day in call_days + [TODAY]:
        t = la_to_utc(datetime.combine(day, time(7)) + timedelta(minutes=random.randint(-30, 60)))
        for code, minutes in SHIFT:
            end = t + timedelta(minutes=minutes + random.randint(-5, 5))
            presence.append(dict(agentId=a["id"], startTime=t, endTime=end, systemPresence=code,
                                 organizationPresenceId=PRESENCE_IDS[code]))
            t = end
            if day == TODAY and t > NOW:
                break
        else:
            presence.append(dict(agentId=a["id"], startTime=t, endTime=None if day == TODAY else t + timedelta(hours=14),
                                 systemPresence="OFFLINE", organizationPresenceId=PRESENCE_IDS["OFFLINE"]))
            continue
        presence[-1]["endTime"] = None  # still in this status right now
save(GOLD, "AgentPrimaryPresence",
     [("agentId", "STRING"), ("startTime", "TIMESTAMP"), ("endTime", "TIMESTAMP"), ("systemPresence", "STRING"),
      ("organizationPresenceId", "STRING")], presence)

# COMMAND ----------

# DBTITLE 1,Summary
from collections import Counter

print(f"Agents: {len(agents)} (mover: {mover['name']}, leaver: {leaver['name']} on {leaver['leave_date']})")
print(f"Calls: {len({s['interactionId'] for s in sessions})} over {DAYS_OF_CALLS} days, metric events: {len(metrics)}")
print(f"PNRs booked on calls: {len(bookings)} (rebooks: {sum(b['rebook'] for b in bookings)}), all PNRs: {len(pnrs)}")
print(f"PNR status mix: {dict(Counter(p['status'] for p in pnrs))}")
