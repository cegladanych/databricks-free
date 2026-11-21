"""Reservations contact-centre statistics – Lakeflow Declarative Pipeline (Python).

Python port of ``Stats.sql``. Every ``CREATE LIVE TABLE`` of the SQL version is a materialized view here:

==========================  ==============================================================================
PassengerCalls              one row per call leg x metric event, enriched with agent, queue, skill, business unit
PassengerCallStats          15-minute interval stats of inbound answered / abandoned calls per queue & skill
CampaignCalls               first leg of every call to a marketing campaign number, matched to PNRs booked on it
AgentPresenceByDay          agent presence (status) periods, bucketed to 30 minutes
CampaignCallsAPI            CampaignCalls reshaped for the call-tracking (campaign attribution) feed
PassengerCallSummary        one row per call leg with all handle-time metrics and a derived CallType
ResAgentsMonthly            monthly roster of reservation agents (start/end of month dept, title, supervisor, HR)
==========================  ==============================================================================

Catalog / schema names come from the pipeline configuration in ``resources/Stats.yml``.
"""
from functools import reduce
from operator import and_

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

# ----------------------------------------------------------------------------------------------------------------------
# Configuration (resources/Stats.yml -> configuration)
# ----------------------------------------------------------------------------------------------------------------------
RAW_CATALOG = spark.conf.get("raw_catalog")
RAW_MAIN_SCHEMA = spark.conf.get("raw_main_schema")  # contact-centre platform (agents, queues, skills ...)
RAW_SUP_SCHEMA = spark.conf.get("raw_sup_schema")  # reservation system / PSS (PNRs, flights, res agents ...)
RAW_GENERIC_SCHEMA = spark.conf.get("raw_generic_schema")  # HR system
GOLD_CATALOG = spark.conf.get("gold_catalog")
GOLD_SCHEMA = spark.conf.get("gold_schema")  # curated contact-centre facts
GOLD_MAINDATA_SCHEMA = spark.conf.get("gold_maindata_schema")  # passenger / account / product dimensions
GOLD_NCF_SCHEMA = spark.conf.get("gold_ncf_schema")  # net fare revenue
REPORTING_CATALOG = spark.conf.get("reporting_catalog")
REPORTING_SCHEMA = spark.conf.get("reporting_schema")


def _read(catalog: str, schema: str, table: str) -> DataFrame:
    return spark.read.table(f"{catalog}.{schema}.{table}")


def contact(table: str) -> DataFrame:
    return _read(GOLD_CATALOG, GOLD_SCHEMA, table)


def contact_raw(table: str) -> DataFrame:
    return _read(RAW_CATALOG, RAW_MAIN_SCHEMA, table)


def reservations(table: str) -> DataFrame:
    return _read(RAW_CATALOG, RAW_SUP_SCHEMA, table)


def hr(table: str) -> DataFrame:
    return _read(RAW_CATALOG, RAW_GENERIC_SCHEMA, table)


def maindata(table: str) -> DataFrame:
    return _read(GOLD_CATALOG, GOLD_MAINDATA_SCHEMA, table)


def net_fare(table: str) -> DataFrame:
    return _read(GOLD_CATALOG, GOLD_NCF_SCHEMA, table)


def reporting(table: str) -> DataFrame:
    return _read(REPORTING_CATALOG, REPORTING_SCHEMA, table)


# ----------------------------------------------------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------------------------------------------------
CALL_KEYS = ["interactionId", "callLegId", "partyId"]
CAMPAIGN_BUSINESS_UNITS = ["Home", "US Inbound Queues"]
COMPANY_EMAIL_DOMAIN = "%example-air.com%"


def col(name: str) -> Column:
    return F.col(name)


def match(left: str, right: str, keys=CALL_KEYS) -> Column:
    """left.k1 = right.k1 AND left.k2 = right.k2 ..."""
    return reduce(and_, [col(f"{left}.{k}") == col(f"{right}.{k}") for k in keys])


def is_current() -> Column:
    return col("dwh_current_flag") == F.lit(True)


def not_deleted() -> Column:
    return col("dwh_delete_flag") == F.lit(False)


def current(df: DataFrame) -> DataFrame:
    """Current, non-deleted version of an SCD source table."""
    return df.where(is_current() & not_deleted())


def floor_to(ts: Column, seconds: int) -> Column:
    """Truncate a timestamp to an N-second interval (FROM_UNIXTIME((UNIX_TIMESTAMP(ts) div N) * N))."""
    return F.timestamp_seconds(F.floor(F.unix_timestamp(ts) / seconds) * seconds)


def metric(name: str, value: Column = None, *, direction: int = None, extra: Column = None) -> Column:
    """CASE WHEN Metric = <name> [AND DirectionCode = <direction>] [AND <extra>] THEN <value> END"""
    cond = col("Metric") == name
    if direction is not None:
        cond = cond & (col("DirectionCode") == direction)
    if extra is not None:
        cond = cond & extra
    return F.when(cond, F.lit(1) if value is None else value)


def first_matching(rules, otherwise: Column) -> Column:
    """Build CASE WHEN ... from an ordered list of (condition, value) tuples."""
    (cond0, val0), *rest = rules
    return reduce(lambda acc, r: acc.when(r[0], r[1]), rest, F.when(cond0, val0)).otherwise(otherwise)


def clean_phone(c: str) -> Column:
    return F.substring(F.regexp_replace(col(c), r"tel:\+", ""), 1, 100)


def email_prefix(c: Column) -> Column:
    return F.substring_index(F.lower(c), "@", 1)


def booking_status(prefix: str) -> Column:
    status, received, confirm = col(f"{prefix}.Status"), col(f"{prefix}.ReceivedAmt"), col(f"{prefix}.ConfirmFlag")
    return (
        F.when(F.upper(status) == "K", "CANCELLED")
        .when(((status == "") & (received > 0)) | (F.upper(confirm) == "Y"), "CONFIRMED")
        .when((status == "") & (received == 0), "OPTION")
        .otherwise("Other")
    )


# ----------------------------------------------------------------------------------------------------------------------
# PassengerCalls
# ----------------------------------------------------------------------------------------------------------------------
@dp.materialized_view(
    name="PassengerCalls",
    comment="Every metric event of every passenger call leg, enriched with agent, queue, skills and business unit.",
)
def passenger_calls():
    # queue the leg was in, with the time window it spent there
    queue = (
        contact("ContactSegments").alias("ss")
        .join(
            contact_raw("res_queues").alias("q"),
            (col("ss.queueId") == col("q.id"))
            & (col("ss.segmentStart") >= col("q.dwh_valid_from"))
            & (col("ss.segmentStart") < col("q.dwh_valid_to")),
            "left",
        )
        .where(col("ss.queueId").isNotNull())
        .groupBy(
            col("ss.interactionId"), col("ss.partyId"), col("ss.callLegId"),
            col("ss.queueId").alias("QueueID"), col("q.name").alias("QueueName"),
            col("q.description").alias("QueueDescription"), col("q.businessUnitId"),
        )
        .agg(F.min("ss.segmentStart").alias("SegmentStartDate"), F.max("ss.segmentEnd").alias("SegmentEndDate"))
    )

    # all skills requested for the leg, ';'-separated
    skills = (
        contact("ContactSkillAssignments").alias("a")
        .join(current(contact_raw("agent_skills")).alias("b"), col("a.skillId") == col("b.id"), "left")
        .groupBy("a.interactionId", "a.callLegId", "a.partyId")
        .agg(F.array_join(F.collect_set("b.name"), ";").alias("SkillName"))
    )

    supervisors = contact_raw("agents").where(is_current()).select("id", "name").distinct()

    # disconnect type of the last segment that has one
    last_disconnect = (
        contact("ContactSegments")
        .where(col("disconnectType").isNotNull())
        .withColumn("MaxEnd", F.max("segmentEnd").over(Window.partitionBy(*CALL_KEYS)))
        .where(col("segmentEnd") == col("MaxEnd"))
        .select(*CALL_KEYS, "disconnectType")
        .distinct()
    )

    event = col("sm.eventTimestamp")
    return (
        contact("ContactSessions").alias("a")
        .join(contact("ContactSessionMetrics").alias("sm"), match("a", "sm"))
        .join(contact("ContactParticipants").alias("pp"), match("a", "pp", ["partyId", "interactionId"]))
        .join(
            contact_raw("agents").alias("us"),
            (col("pp.agentId") == col("us.id"))
            & (event >= col("us.dwh_valid_from"))
            & (event <= col("us.dwh_valid_to")),
            "left",
        )
        .join(supervisors.alias("usm"), col("us.supervisorId") == col("usm.id"), "left")
        .join(
            queue.alias("que"),
            match("sm", "que") & event.between(col("que.SegmentStartDate"), col("que.SegmentEndDate")),
            "left",
        )
        .join(skills.alias("skill"), match("sm", "skill"), "left")
        .join(current(contact_raw("business_units")).alias("queDiv"), col("queDiv.id") == col("que.businessUnitId"), "left")
        .join(current(contact_raw("business_units")).alias("usDiv"), col("usDiv.id") == col("us.businessUnitId"), "left")
        .join(last_disconnect.alias("df"), match("a", "df"), "left")
        .where(F.to_date(F.from_utc_timestamp(event, "PST")) >= F.lit("2021-12-10").cast("date"))
        .select(
            F.from_utc_timestamp(event, "PST").alias("EventDateTime_LAX"),
            F.from_utc_timestamp(event, "Australia/Sydney").alias("EventDateTime_SYD"),
            F.from_utc_timestamp(event, "Europe/London").alias("EventDateTime_LHR"),
            event.cast("timestamp").alias("EventDateTime_UTC"),
            F.when(col("a.direction") == "inbound", 1).when(col("a.direction") == "outbound", 2).otherwise(3)
            .alias("DirectionCode"),
            "a.interactionId",
            "a.callLegId",
            "a.partyId",
            col("sm.value"),
            col("sm.name").alias("Metric"),
            clean_phone("a.ani").alias("CallerNumber"),
            clean_phone("a.dnis").alias("DialedNumber"),
            col("pp.purpose").alias("PurposeCode"),
            col("pp.agentId").alias("agentId"),
            col("us.staffNumber"),
            col("us.name").alias("AgentName"),
            col("que.QueueID"),
            col("que.QueueName"),
            F.lit("NULL").alias("QueueDescription"),
            col("skill.SkillName"),
            col("queDiv.name").alias("BusinessUnit"),
            col("usDiv.name").alias("AgentBusinessUnit"),
            "a.routingRing",
            "a.agentBullseyeRing",
            col("usm.name").alias("SupervisorName"),
            "us.hireDate",
            "us.department",
            "us.title",
            "a.usedRouting",
            "a.mediaType",
            "a.remote",
            "df.disconnectType",
        )
    )


# ----------------------------------------------------------------------------------------------------------------------
# PassengerCallStats
# ----------------------------------------------------------------------------------------------------------------------
STATS_SKILLS = ["Consumer", "Travel Agent", "US - Round the World", "US - Irregular Ops"]


def dept_group() -> Column:
    q, s = col("QueueName"), col("SkillName")
    rules = [
        (((q == "Sales") & (s == "Consumer")) | ((q == "Sales") & s.isNull())
         | q.isin("Sales", "US - Internal Direct Sales - 2117"), "Direct Sales"),
        (((q == "Sales") & (s == "Travel Agent")) | (q == "US - Internal TA Sales - 2118")
         | ((q == "Sales - TA") & ((s == "Travel Agent") | s.isNull())), "TA Sales"),
        (((q == "Service") & s.isin("Consumer")) | ((q == "Service") & s.isNull())
         | (q == "US - Internal Service Direct 3083"), "Service"),
        (((q == "Service") & (s == "Travel Agent")) | (q == "US - Internal Service TA 3084"), "Service"),
        ((q == "Service") & s.isin("US - Round the World", "Irregular Ops"), "Service"),
        (q == "Round the World", "Service"),
        (q.isin("Payment", "US - Internal Existing Groups 1260", "Round the World", "Charter Flights"), "Service"),
        (q.isin("Customer Relations", "US - Internal Customer Relations", "Customer Relations - Covid19"),
         "Customer Relations"),
        (q.isin("Air Customer Service", "US - Internal Air - 1206", "US - Air VIP"), "Air Customer Service"),
        (q.isin("Enroute", "US - Internal Enroute 1208"), "Air Operations"),
        (q.isin("US - Air Resolutions"), "Air Resolution"),
        (q.isin("Air Back Office"), "Air Back Office"),
        (q.isin("Inside Sales"), "Inside Sales"),
        (q.isin("New Groups"), "Group Sales"),
        (q.isin("Passenger Relations"), "Passenger Services"),
        (q.isin("WFM - 1501, 1502"), "WFM"),
        (q.isin("Chat"), "Chat"),
        (q.isin("Outbound"), "Outbound Sales"),
        (q.isin("Operator"), "Front Desk"),
    ]
    return first_matching(rules, otherwise=q)


@dp.materialized_view(
    name="PassengerCallStats",
    comment="15-minute interval statistics of inbound answered / abandoned passenger calls by queue, skill and department.",
)
def passenger_call_stats():
    calls = spark.read.table("PassengerCalls")

    stats_skills = (
        contact("ContactSkillAssignments").alias("a")
        .join(contact_raw("agent_skills").alias("b"), col("a.skillId") == col("b.id"), "left")
        .where(col("b.name").isin(*STATS_SKILLS))
        .select("a.interactionId", "a.callLegId", "a.partyId", "a.skillId", col("b.name").alias("SkillName"))
        .distinct()
    )

    leg_group = [
        "DirectionCode", *CALL_KEYS, "usedRouting", "routingRing", "agentBullseyeRing", "AgentName", "PurposeCode",
        "agentId", "staffNumber", "BusinessUnit", "CallerNumber", "DialedNumber", "QueueName", "QueueID", "SkillName",
        "department", "title", "hireDate", "mediaType",
    ]
    seconds = col("value") / 1000
    legs = (
        calls.where(col("DirectionCode") == 1)
        .groupBy(*leg_group)
        .agg(
            F.min("EventDateTime_LAX").alias("StartDateTime"),
            F.min("EventDateTime_UTC").alias("EventDateTime_UTC"),
            F.sum(metric("nOffered")).alias("Offered"),
            F.max(metric("tAnswered", seconds)).alias("Answered_Second"),
            F.max(metric("tAbandon", seconds)).alias("Abandoned_Second"),
            F.max(metric("tHandle", seconds)).alias("Handled_Second"),
            F.max(metric("tShortAbandon", seconds)).alias("ShortAbandon_Second"),
        )
        .withColumnRenamed("SkillName", "SkillName_Combined")
    )

    over_sla = calls.where(col("Metric") == "nOverSla").select(*CALL_KEYS).distinct()

    call_outcomes = (
        contact("ContactSegments").alias("a")
        .join(contact_raw("call_outcome_codes").alias("b"), col("a.callOutcomeCode") == col("b.id"))
        .where(col("a.callOutcomeCode").isNotNull() & col("b.dwh_current_flag") & ~col("b.dwh_delete_flag"))
        .select("a.interactionId", "a.partyId", "a.callLegId", "a.queueId", "a.callOutcomeCode",
                col("b.name").alias("CallOutcomeName"))
        .distinct()
    )

    answered_or_abandoned = (
        legs.alias("a")
        .join(stats_skills.alias("b"), match("a", "b"), "left")
        .join(over_sla.alias("c"), match("a", "c"), "left")
        .join(call_outcomes.alias("d"), match("a", "d") & (col("a.QueueID") == col("d.queueId")), "left")
        .where(col("a.Answered_Second").isNotNull() | col("a.Abandoned_Second").isNotNull())
        .select(
            "a.*",
            col("b.SkillName"),
            F.when(col("c.interactionId").isNotNull(), 1).otherwise(0).alias("Answered_OverSLA"),
            col("d.CallOutcomeName"),
        )
    )

    wait = F.coalesce(col("Answered_Second"), col("Abandoned_Second"), F.lit(0))
    offered_at = lambda ts: F.timestamp_seconds((F.unix_timestamp(col(ts)) - wait).cast("long"))
    flag = lambda c: F.when(col(c).isNotNull(), 1).otherwise(0)
    zero = lambda c: F.coalesce(col(c), F.lit(0))

    per_leg = answered_or_abandoned.select(
        col("StartDateTime"),
        floor_to(col("StartDateTime"), 900).alias("IntervalStart"),
        floor_to(col("EventDateTime_UTC"), 900).alias("IntervalStart_UTC"),
        F.to_date("StartDateTime").alias("Date"),
        "QueueName",
        "SkillName",
        flag("Answered_Second").alias("AnsweredCount"),
        zero("Answered_Second").alias("AnsweredTimeTotal"),
        flag("Abandoned_Second").alias("AbandonedCount"),
        zero("Abandoned_Second").alias("AbandonedTimeTotal"),
        zero("Handled_Second").alias("HandledTimeTotal"),
        zero("Answered_OverSLA").alias("Answered_OverSLA"),
        dept_group().alias("DeptGroup"),
        "DirectionCode",
        F.when((col("routingRing") > 1) & (col("agentBullseyeRing") > 1), 1).otherwise(0).alias("OverFlowCount"),
        F.when(col("department") == "Direct Sales Remote", "Direct Sales").otherwise(col("department"))
        .alias("Department_OF_Taken"),
        col("title").alias("Title_OF_Taken"),
        "usedRouting",
        "routingRing",
        "agentBullseyeRing",
        "CallOutcomeName",
        "SkillName_Combined",
        floor_to(offered_at("StartDateTime"), 900).alias("Offered_Interval"),
        floor_to(offered_at("EventDateTime_UTC"), 900).alias("Offered_Interval_UTC"),
        "mediaType",
        "BusinessUnit",
        flag("ShortAbandon_Second").alias("ShortAbandonedCount"),
        zero("ShortAbandon_Second").alias("ShortAbandonedTimeTotal"),
    )

    sums = ["AnsweredCount", "AnsweredTimeTotal", "AbandonedCount", "AbandonedTimeTotal", "HandledTimeTotal",
            "Answered_OverSLA", "OverFlowCount", "ShortAbandonedCount", "ShortAbandonedTimeTotal"]
    group = ["IntervalStart", "IntervalStart_UTC", "Date", "QueueName", "SkillName", "DeptGroup", "DirectionCode",
             "Department_OF_Taken", "Title_OF_Taken", "usedRouting", "routingRing", "agentBullseyeRing",
             "CallOutcomeName", "SkillName_Combined", "Offered_Interval", "Offered_Interval_UTC", "mediaType",
             "BusinessUnit"]
    return (
        per_leg.groupBy(*group)
        .agg(*[F.sum(zero(c)).alias(c) for c in sums])
        .select(
            "IntervalStart", "IntervalStart_UTC", "Date", "QueueName", "SkillName",
            "AnsweredCount", "AnsweredTimeTotal", "AbandonedCount", "AbandonedTimeTotal", "HandledTimeTotal",
            "Answered_OverSLA", "DeptGroup", "DirectionCode", "OverFlowCount", "Department_OF_Taken",
            "Title_OF_Taken", "usedRouting", "routingRing", "agentBullseyeRing", "CallOutcomeName",
            "SkillName_Combined", "Offered_Interval", "Offered_Interval_UTC", "mediaType", "BusinessUnit",
            "ShortAbandonedCount", "ShortAbandonedTimeTotal",
        )
    )


# ----------------------------------------------------------------------------------------------------------------------
# CampaignCalls
# ----------------------------------------------------------------------------------------------------------------------
RES_SALES_QUEUES = ["Sales", "US - Internal Direct Sales - 2117", "Sales - TA", "US - Internal TA Sales - 2118"]
RES_SERVICE_QUEUES = ["Service", "US - Internal Service Direct 3083", "US - Internal Service TA 3084",
                      "Round the World", "Payment", "US - Internal Existing Groups 1260", "Charter Flights"]


def net_fare_revenue() -> DataFrame:
    """Net fare revenue per PNR = gross - discount - voucher - voucher redemption (USD and local currency)."""
    grp = col("da.fpaAccountGroup")

    def total(group_name: str, amount: str, sign: int = 1) -> Column:
        return F.sum(F.when(grp == group_name, col(f"n.{amount}")).otherwise(F.lit(0))) * sign

    return (
        net_fare("NetFareRevenue").alias("n")
        .join(maindata("DimPassenger").alias("dp"), col("n.JourneyPaxKey") == col("dp.JourneyPaxKey"), "left")
        .join(maindata("DimGLAccount").alias("da"), col("n.AccountKey") == col("da.AccountKey"), "left")
        .join(maindata("DimFareProduct").alias("dpt"), col("n.ProductKey") == col("dpt.ProductKey"), "left")
        .where(
            ~col("da.AccountGroup").isin("Cost", "Adjustment", "Commission", "Voucher redemption")
            & ~col("dpt.ProductCategory").isin("CancelRevenue")
            & ((col("dpt.IsGiftExpensed") == F.lit(False)) | col("dpt.IsGiftExpensed").isNull())
            & ((col("dp.IsPaxCancelled") != 1) | (col("da.AccountType") != "Inflight gifts"))
        )
        .groupBy("dp.TravelPnr", "dp.BookingDateTime")
        .agg(
            (total("Gross", "AmountUSD") - total("Discount", "AmountUSD", -1) - total("Voucher", "AmountUSD", -1)
             - total("Voucher redemption", "AmountUSD")).alias("NetFareRevenue_USD"),
            (total("Gross", "AmountLocalCcy") - total("Discount", "AmountLocalCcy", -1)
             - total("Voucher", "AmountLocalCcy", -1) - total("Voucher redemption", "AmountLocalCcy"))
            .alias("NetFareRevenue_LC"),
        )
    )


def pnr_bookings() -> DataFrame:
    """PNRs with booking status, itinerary, net fare and the contact-centre agent that created them."""
    agents = contact_raw("agents").where(
        (col("state") == "active")
        & (~F.lower(col("title")).like("%test%") | ~F.lower(col("department")).like("%test%")
           | ~F.lower(col("name")).like("%test%"))
    )
    res_agents = (
        reservations("res_agents").where(is_current()).select("agentId", "firstName", "lastName", "email").distinct()
    )
    created = col("a.pnrCreatedDate")
    return (
        current(reservations("pnr_header")).alias("a")
        .where(F.to_date(created) >= F.lit("2021-01-01").cast("date"))
        .join(current(reservations("pnr_sales")).alias("b"), col("a.pnr") == col("b.pnr"), "left")
        .join(current(reservations("pnr_agent_assignments")).alias("c"), col("a.pnr") == col("c.pnr"), "left")
        .join(current(reservations("flights")).alias("d"), col("b.FlightNumber") == col("d.FlightNumber"), "left")
        .join(res_agents.alias("e"), col("c.assignedAgentId") == col("e.agentId"), "left")
        .join(
            agents.alias("f"),
            (email_prefix(col("e.email")) == email_prefix(col("f.email")))
            & (created >= F.from_utc_timestamp(col("f.dwh_valid_from"), "America/Los_Angeles"))
            & (created < F.from_utc_timestamp(col("f.dwh_valid_to"), "America/Los_Angeles")),
            "left",
        )
        .join(net_fare_revenue().alias("j"), col("a.pnr") == col("j.TravelPnr"), "left")
        .select(
            col("a.pnr"),
            created.alias("BookingDate"),
            booking_status("b").alias("BookingStatus"),
            col("d.RouteName").alias("Itinerary"),
            col("f.id").alias("agentid_booking"),
            col("j.NetFareRevenue_USD"),
            F.row_number().over(Window.partitionBy("a.pnr").orderBy(col("f.dwh_valid_from").desc())).alias("rn"),
        )
    )


def rebooked_pnrs(bookings: DataFrame) -> DataFrame:
    """New PNRs created the same day another PNR of the same frequent flyer was cancelled."""
    with_ffp = (
        bookings.alias("a")
        .join(current(reservations("pnr_passengers")).alias("p"), col("a.pnr") == col("p.pnr"), "left")
        .select("a.*", "p.ffpNumber")
        .union(
            bookings.alias("a")
            .join(current(reservations("pnr_cancelled_passengers")).alias("p"), col("a.pnr") == col("p.pnr"), "left")
            .select("a.*", "p.ffpNumber")
        )
        .distinct()
    )
    return (
        with_ffp.alias("a")
        .join(current(reservations("pnr_cancelled_passengers")).alias("c"), col("a.ffpNumber") == col("c.ffpNumber"))
        .join(current(reservations("pnr_sales")).alias("s"), col("c.pnr") == col("s.pnr"))
        .where((col("a.pnr") > col("c.pnr")) & (F.to_date("a.BookingDate") == F.to_date("s.CancelDate")))
        .select(col("a.pnr").alias("rebook_pnr"))
        .distinct()
    )


@dp.materialized_view(
    name="CampaignCalls",
    comment="First leg of each inbound call to an active campaign number, matched to the PNR booked during the call.",
)
def campaign_calls():
    calls = spark.read.table("PassengerCalls")

    # first / last event of each inbound call to an active campaign number
    campaign_numbers = contact("CampaignPhoneNumbers").where(col("active_flag") == 1)
    campaign_contacts = (
        calls.alias("a")
        .join(campaign_numbers.alias("b"), col("a.DialedNumber") == col("b.phone_number"))
        .where(
            (col("a.BusinessUnit").isin(*CAMPAIGN_BUSINESS_UNITS) | col("a.BusinessUnit").isNull())
            & (col("a.DirectionCode") == 1)
        )
        .groupBy("a.interactionId", "a.CallerNumber", "a.DialedNumber")
        .agg(F.min("a.EventDateTime_LAX").alias("Min_CallDateTime"), F.max("a.EventDateTime_LAX").alias("CallEndTime"))
    )

    # answered / abandoned outcome of each call (first one wins)
    outcomes = calls.where(
        col("Metric").isin("tAnswered", "tAbandon") & (col("DirectionCode") == 1) & (F.length("DialedNumber") == 11)
    )
    call_base = (
        campaign_contacts.alias("a")
        .join(outcomes.alias("b"), col("a.interactionId") == col("b.interactionId"), "left")
        .select(
            col("a.interactionId"),
            col("b.department"),
            col("b.title"),
            col("b.SkillName"),
            col("a.Min_CallDateTime"),
            col("a.CallEndTime"),
            F.coalesce(col("b.EventDateTime_LAX"), col("a.Min_CallDateTime")).alias("CallStart"),
            F.when(col("b.Metric").isNull(), "Abandoned_BeforeQueue")
            .when(col("b.Metric") == "tAbandon", "Abandoned_InQueue")
            .when(col("b.Metric") == "tAnswered", "Answered")
            .alias("Metric"),
            F.coalesce(col("b.CallerNumber"), col("a.CallerNumber")).alias("CallerNumber"),
            F.coalesce(col("b.DialedNumber"), col("a.DialedNumber")).alias("DialedNumber"),
            col("b.QueueName"),
            col("b.agentId"),
            F.row_number().over(
                Window.partitionBy("a.interactionId").orderBy(col("b.interactionId"), col("b.EventDateTime_LAX"))
            ).alias("RowOrder"),
        )
        .where(col("RowOrder") == 1)
    )

    dept, title = col("department"), col("title")
    consultant = title.like("%Consultant%")
    first_legs = call_base.select(
        "interactionId",
        first_matching(
            [
                (((dept == "Direct Sales") & consultant) | (title == "Travel Sales Consultant"), "Direct Sales"),
                (((dept == "TA Sales") & consultant) | (title == "Travel Agent Sales Consultant"), "TA Sales"),
                (((dept == "Outbound") & consultant) | (title == "Outbound Sales Consultant"), "Outbound Sales"),
                ((dept.isin("Chat", "Chat Sales") & consultant) | (title == "Web Chat Sales Consultant"), "Chat Sales"),
            ],
            otherwise=dept,
        ).alias("department"),
        "SkillName",
        "Min_CallDateTime",
        "Metric",
        "CallStart",
        "CallEndTime",
        "CallerNumber",
        "DialedNumber",
        "agentId",
        F.when(col("QueueName").isin(*RES_SALES_QUEUES), "Res-Sales")
        .when(col("QueueName").isin(*RES_SERVICE_QUEUES), "Res-Service")
        .otherwise("Other")
        .alias("Dept"),
    )

    bookings = pnr_bookings()
    rebooks = rebooked_pnrs(bookings)

    status_rank = (
        F.when(col("b.BookingStatus").isin("CONFIRMED", "FINAL"), "A")
        .when(col("b.BookingStatus") == "OPTION", "B")
        .when(col("b.BookingStatus") == "CANCELLED", "C")
        .otherwise("D")
    )
    return (
        first_legs.alias("a")
        .join(
            bookings.where(col("rn") == 1).alias("b"),
            (col("a.agentId") == col("b.agentid_booking"))
            & col("b.BookingDate").between(col("a.CallStart"), col("a.CallEndTime")),
            "left",
        )
        .join(rebooks.alias("rb"), col("b.pnr") == col("rb.rebook_pnr"), "left")
        .select(
            col("a.interactionId"),
            col("a.department"),
            col("a.SkillName").alias("skillname"),
            F.lit("").alias("CampaignID"),
            col("a.Min_CallDateTime").alias("Min_CallStart"),
            F.to_utc_timestamp(col("a.Min_CallDateTime"), "PST").alias("Min_CallStartUTC"),
            col("a.Metric"),
            col("a.CallStart"),
            F.to_utc_timestamp(col("a.CallStart"), "PST").alias("CallStartUTC"),
            col("a.CallerNumber").alias("Calling_Phone_Number"),
            F.when(col("a.Dept") == "Res-Sales", 1).otherwise(0).alias("Sales_Call"),
            F.when(col("a.Dept") == "Res-Service", 1).otherwise(0).alias("Service_Call"),
            col("a.DialedNumber").alias("TFN"),
            col("b.pnr").alias("BookingNumber"),
            F.when(col("rb.rebook_pnr").isNotNull(), "Rebook").when(col("b.pnr").isNotNull(), "New")
            .otherwise("Non-Booking").alias("Order_Type"),
            col("b.NetFareRevenue_USD"),
            col("b.Itinerary"),
            F.row_number().over(
                Window.partitionBy("a.interactionId").orderBy(col("a.interactionId"), col("b.pnr"), status_rank)
            ).alias("BookingOrder"),
        )
        .where((col("BookingOrder") == 1) & (F.to_date("Min_CallStart") >= F.lit("2021-12-22").cast("date")))
        .drop("BookingOrder")
    )


# ----------------------------------------------------------------------------------------------------------------------
# AgentPresenceByDay
# ----------------------------------------------------------------------------------------------------------------------
@dp.materialized_view(
    name="AgentPresenceByDay",
    comment="Agent presence periods (system + organisation presence) with 30-minute bucketed start / end.",
)
def agent_presence_by_day():
    by_agent = Window.partitionBy("agentId").orderBy(col("startTime").asc())
    next_end = F.lead("endTime").over(by_agent)
    now = F.current_timestamp()
    presence = contact("AgentPrimaryPresence").select(
        "startTime",
        "agentId",
        F.coalesce(col("endTime"), next_end, now).alias("endTime"),
        floor_to(col("startTime"), 1800).alias("startDate"),
        F.coalesce(floor_to(col("endTime"), 1800), floor_to(next_end, 1800), floor_to(now, 1800)).alias("endDate"),
        "systemPresence",
        "organizationPresenceId",
    )

    statuses = contact_raw("agent_presence_statuses")
    labels = statuses.select(
        F.from_json("languageLabels", "struct<en:string,en_US:string>").getField("en_US").alias("presence"),
        col("id").alias("presenceId"),
    )

    seconds = F.unix_timestamp(col("ursh.endTime")) - F.unix_timestamp(col("ursh.startTime"))
    days = F.datediff(F.to_date("ursh.endDate"), F.to_date("ursh.startDate"))
    return (
        presence.alias("ursh")
        .join(current(statuses).alias("dsp"), col("ursh.organizationPresenceId") == col("dsp.id"))
        .join(labels.alias("ll1"), col("ll1.presenceId") == col("ursh.organizationPresenceId"), "left")
        .select(
            col("ursh.agentId"),
            col("ursh.startTime"),
            F.coalesce(col("ursh.endTime"), now).alias("endTime"),
            col("ursh.startDate"),
            col("ursh.endDate"),
            F.when(seconds < 0, 0).otherwise(seconds).alias("duration_s"),
            F.when(days < 0, 0).otherwise(days).alias("duration_d"),
            col("dsp.id").alias("primaryId"),
            col("dsp.systemPresence").alias("primary"),
            col("ursh.organizationPresenceId").alias("secondaryId"),
            col("ll1.presence").alias("secondary"),
        )
        .distinct()
    )


# ----------------------------------------------------------------------------------------------------------------------
# CampaignCallsAPI
# ----------------------------------------------------------------------------------------------------------------------
@dp.materialized_view(
    name="CampaignCallsAPI",
    comment="Campaign calls in the shape expected by the call-tracking (campaign attribution) feed.",
)
def campaign_calls_api():
    booking = col("I.BookingNumber")
    status = col("S.StatusText")
    return (
        spark.read.table("CampaignCalls").alias("I")
        .join(reporting("PnrRevenueGrouped").alias("G"), booking == col("G.pnr"), "left")
        .join(reporting("PnrBookingStatus").alias("S"), booking == col("S.pnr"), "left")
        .where((col("I.Sales_Call") != 0) | (col("I.Service_Call") != 0) | booking.isNotNull())
        .select(
            F.date_format("I.Min_CallStart", "yyyy-MM-dd hh:mm:ss a").alias("call_start_time"),
            F.concat(F.lit("+"), col("I.Calling_Phone_Number")).alias("calling_phone_number"),
            col("I.Sales_Call").alias("sales_call"),
            col("I.Service_Call").alias("service_call"),
            col("I.TFN"),
            F.coalesce(booking.cast("string"), F.lit("")).alias("booking_number"),
            col("I.Order_Type").alias("order_type"),
            F.coalesce(col("I.Itinerary"), F.lit("")).alias("flight_number"),
            F.when(booking > 0, 1).otherwise(0).alias("booking"),
            F.coalesce(col("G.GrossFareAmt"), F.lit(0.0)).alias("revenue"),
            F.when(status == "OPTION", "CourtesyHold")
            .when(status == "FINAL", "FullPayment")
            .when(status == "CONFIRMED", "Deposit")
            .when(status == "CANCELLED", "Cancelled")
            .otherwise("NA")
            .alias("booking_status"),
            col("I.interactionId").alias("interactionId"),
            col("I.department").alias("department"),
            col("I.skillname").alias("skillname"),
            col("I.CampaignID").alias("CampaignID"),
            col("I.NetFareRevenue_USD").alias("NetFareRevenue_USD"),
        )
    )


# ----------------------------------------------------------------------------------------------------------------------
# PassengerCallSummary
# ----------------------------------------------------------------------------------------------------------------------
def call_type() -> Column:
    direction, media = col("b.DirectionCode"), F.lower(col("b.mediaType"))
    number, purpose = col("b.DialedNumber"), F.lower(col("b.PurposeCode"))
    answered, handled, abandoned = col("a.AnsweredValue"), col("a.HandledValue"), col("a.AbandonedValue")
    has_letters = F.lower(number).rlike(".*[A-Za-z].*")
    voice, message = media == "voice", media == "message"
    rules = [
        ((direction == 2) & (answered != 0) & (media == "callback"), "Callback"),
        ((direction == 2) & (F.length(number) > 6) & voice & ~has_letters, "Outbound_Passenger"),
        ((direction == 2) & number.rlike(r".*\*86") & voice, "VM"),
        ((direction == 2) & voice & ((F.length(number) <= 6) | has_letters | number.isNull()), "Outbound_Internal"),
        (((direction == 1) & (purpose == "user") & voice)
         | ((direction == 1) & (answered == 0) & (handled > 0) & voice), "Direct_Ext"),
        ((direction == 1) & (purpose == "agent") & (answered != 0) & voice, "Inbound_Queue"),
        ((direction == 1) & (purpose == "agent") & (answered != 0) & message, "SMS_Inbound"),
        ((direction == 2) & (purpose == "agent") & message, "SMS_Outbound"),
        (abandoned != 0, "Abandoned"),
    ]
    return first_matching(rules, otherwise=F.lit("Other"))


@dp.materialized_view(
    name="PassengerCallSummary",
    comment="One row per call leg with talk / hold / wrap-up / handle times and a derived call type.",
    partition_cols=["partition_date"],
)
def passenger_call_summary():
    calls = spark.read.table("PassengerCalls")

    # legs that were in both US business units -> reported as 'US Inbound Queues'
    split_business_unit = (
        calls.where(col("BusinessUnit").isin("US Inbound Queues", "Home"))
        .groupBy(*CALL_KEYS)
        .agg(F.countDistinct("BusinessUnit").alias("BusinessUnitCt"))
        .where(col("BusinessUnitCt") > 1)
    )

    v = col("value")
    measures = {
        "AnsweredInbound": F.sum(metric("tAnswered", direction=1)),
        "AnsweredValue": F.max(metric("tAnswered", v, direction=1)),
        "AbandonedValue": F.max(metric("tAbandon", v)),
        "Answered_OverSLA": F.max(F.when(col("Metric") == "nOverSla", v).otherwise(0)),
        "TotalTalkTimeValue": F.max(metric("tTalkComplete", v)),
        "HandledValue": F.max(metric("tHandle", v)),
        "ACW_Time": F.max(metric("tAcw", v)),
        "HeldValue": F.max(metric("tHeldComplete", v)),
        "AnsweredHeld": F.sum(metric("tHeldComplete", extra=v != 0)),
        "HeldValueTrue": F.max(metric("tHeldComplete", v, extra=v != 0)),
        "Transferred": F.sum(metric("nTransferred", extra=v != 0)),
        "CallBack_Count": F.sum(metric("tAnswered", direction=2)),
        "CallBack_AnsValue": F.max(metric("tAnswered", v, direction=2)),
        "ShortAbandon_Inbound": F.sum(metric("tShortAbandon", direction=1)),
        "ShortAbandon_Value": F.max(metric("tShortAbandon", v, direction=1)),
    }

    legs = (
        calls.alias("a")
        .join(split_business_unit.alias("b"), match("a", "b"), "left")
        .where(
            col("a.PurposeCode").isin("user", "agent")
            | (col("a.PurposeCode").isin("acd") & col("a.Metric").isin("tAbandon", "tShortAbandon"))
        )
        .select(
            "a.*",
            F.when(col("b.interactionId").isNull(), col("a.BusinessUnit")).otherwise(F.lit("US Inbound Queues"))
            .alias("LegBusinessUnit"),
        )
        .drop("BusinessUnit")
        .withColumnRenamed("LegBusinessUnit", "BusinessUnit")
        .groupBy(
            "DirectionCode", *CALL_KEYS, "AgentName", "PurposeCode", "agentId", "staffNumber", "CallerNumber",
            "DialedNumber", "QueueName", "BusinessUnit", "AgentBusinessUnit", "routingRing", "agentBullseyeRing",
            "SupervisorName", "hireDate", "department", "title", "mediaType", "SkillName",
        )
        .agg(F.min("EventDateTime_LAX").alias("StartDateTime"), *[m.alias(n) for n, m in measures.items()])
    )

    first_record = legs.groupBy(*CALL_KEYS).agg(
        F.min("StartDateTime").alias("MinStartDateTime"),
        *[F.max(F.coalesce(col(n), F.lit(0))).alias(n) for n in measures],
    )

    as_int = {"AnsweredInbound", "Answered_OverSLA", "AnsweredHeld", "Transferred", "CallBack_Count",
              "ShortAbandon_Inbound", "ShortAbandon_Value"}
    return (
        first_record.alias("a")
        .join(legs.alias("b"), match("a", "b") & (col("a.MinStartDateTime") == col("b.StartDateTime")))
        .select(
            "a.interactionId", "a.callLegId", "a.partyId",
            col("a.MinStartDateTime").alias("StartDateTime"),
            "b.DirectionCode", "b.CallerNumber", "b.DialedNumber", "b.QueueName", "b.AgentName", "b.hireDate",
            "b.department", "b.title", "b.PurposeCode", "b.agentId", "b.staffNumber", "b.BusinessUnit",
            "b.AgentBusinessUnit", "b.SupervisorName",
            col("b.routingRing").cast("int").alias("routingRing"),
            col("b.agentBullseyeRing").cast("int").alias("agentBullseyeRing"),
            "b.mediaType",
            *[(col(f"a.{n}").cast("int") if n in as_int else col(f"a.{n}")).alias(n) for n in measures],
            call_type().alias("CallType"),
            "b.SkillName",
            F.to_date("a.MinStartDateTime").alias("partition_date"),
        )
    )


# ----------------------------------------------------------------------------------------------------------------------
# ResAgentsMonthly
# ----------------------------------------------------------------------------------------------------------------------
AGENT_COLS = ["id", "staffNumber", "name", "email", "hireDate", "businessUnitName", "department", "title", "state",
              "supervisorId", "dwh_delete_flag", "dwh_current_flag", "dwh_valid_from", "dwh_valid_to",
              "locationId1", "locationId2"]


def _month_edges() -> DataFrame:
    """Months since 2014 with the start-of-month (9th, 00:00) and end-of-month snapshot timestamps."""
    this_month = F.date_trunc("month", F.current_date())
    month = col("month_start")
    return (
        reporting("dim_date")
        .select(F.date_trunc("month", col("Date")).alias("month_start"))
        .where((month >= F.lit("2014-01-01").cast("timestamp")) & (month <= this_month))
        .distinct()
        .select(
            month,
            F.date_add(month, 8).cast("timestamp").alias("som_ts"),
            F.add_months(month, 1).cast("timestamp").alias("eom_ts"),
            F.when(month < this_month, "HISTORICAL")
            .when(F.current_date() < F.date_add(month, 8), "CURRENT_PRE_9")
            .when(F.current_date() == F.last_day(F.current_date()), "CURRENT_LAST_DAY")
            .otherwise("CURRENT_MID_MONTH")
            .alias("month_status"),
        )
    )


def _agent_snapshot(edges: DataFrame, ts_col: str) -> DataFrame:
    """Active agent versions valid at <ts_col> of each month since their hire month."""
    return (
        contact_raw("agents").alias("u")
        .join(
            edges.alias("e"),
            (col("u.dwh_valid_from") <= col(f"e.{ts_col}"))
            & (col("u.dwh_valid_to") > col(f"e.{ts_col}"))
            & (col("e.month_start") >= F.date_trunc("month", col("u.hireDate"))),
        )
        .where(col("u.state") == "active")
        .select(col("e.month_start").alias("month"), *[col(f"u.{c}") for c in AGENT_COLS])
    )


def _for_status(df: DataFrame, edges: DataFrame, *statuses: str) -> DataFrame:
    return (
        df.alias("x")
        .join(edges.alias("me"), (col("me.month_start") == col("x.month")) & col("me.month_status").isin(*statuses))
        .select("x.*")
    )


def _hr_records() -> DataFrame:
    """HR staff history; overlapping validity windows are stretched to the next version."""
    valid_from = F.when(col("dwh_valid_from") == F.lit("1900-01-01 00:00:00").cast("timestamp"), col("dwh_load_date")) \
        .otherwise(col("dwh_valid_from"))
    base = (
        hr("hr_staff")
        .where(not_deleted() & col("state").isNotNull())
        .select(
            F.lpad("staffNumber", 11, "0").alias("staffNumber"),
            "state",
            F.lower(F.trim("workEmail")).alias("workEmail"),
            "name",
            valid_from.alias("clean_valid_from"),
            "dwh_valid_to",
            "leaveDate",
        )
    )
    next_from = F.lead("clean_valid_from").over(Window.partitionBy("staffNumber", "workEmail").orderBy("clean_valid_from"))
    return base.withColumn("next_valid_from", next_from).select(
        "staffNumber",
        col("state").alias("workersCompCodeState"),
        "name",
        "leaveDate",
        "workEmail",
        col("clean_valid_from").alias("dwh_valid_from"),
        F.when(col("next_valid_from").isNotNull() & (col("dwh_valid_to") < col("next_valid_from")),
               col("next_valid_from")).otherwise(col("dwh_valid_to")).alias("dwh_valid_to"),
    )


def _local_part(email: Column, minus_one: bool) -> Column:
    """lower(substring(email, 0, charindex('@', email) [- 1]))"""
    length = F.locate("@", email) - (1 if minus_one else 0)
    return F.lower(email.substr(F.lit(0), length))


@dp.materialized_view(
    name="ResAgentsMonthly",
    comment="Monthly roster of reservation agents: start / end of month department, title, supervisor and HR state.",
)
def res_agents_monthly():
    edges = _month_edges()
    som_raw = _agent_snapshot(edges, "som_ts")
    eom_raw = _agent_snapshot(edges, "eom_ts")
    prev_eom = eom_raw.withColumn("month", F.add_months("month", 1).cast("timestamp"))

    final_som = _for_status(som_raw, edges, "HISTORICAL", "CURRENT_LAST_DAY", "CURRENT_MID_MONTH") \
        .unionByName(_for_status(prev_eom, edges, "CURRENT_PRE_9"))
    final_eom = (
        _for_status(eom_raw, edges, "HISTORICAL", "CURRENT_LAST_DAY")
        .unionByName(_for_status(prev_eom, edges, "CURRENT_PRE_9"))
        .unionByName(_for_status(final_som, edges, "CURRENT_MID_MONTH"))
    )

    current_names = contact_raw("agents").where(is_current()).select("name", "id").distinct()
    user_base_pre = (
        final_som.alias("s")
        .join(final_eom.alias("e"), (col("s.id") == col("e.id")) & (col("s.month") == col("e.month")), "left")
        .join(current_names.alias("s1"), col("s.supervisorId") == col("s1.id"), "left")
        .join(current_names.alias("s2"), col("e.supervisorId") == col("s2.id"), "left")
        .select(
            "s.month", "s.id", "s.dwh_valid_from", "s.dwh_valid_to", "s.staffNumber", "s.name", "s.email",
            "s.hireDate", "s.businessUnitName",
            col("s.department").alias("Dept_StartofMonth"),
            col("e.department").alias("Dept_EodMonth"),
            col("s.title").alias("Title_StartofMonth"),
            col("e.title").alias("Title_EodofMonth"),
            col("s1.id").alias("supervisorId_StartOfMonth"),
            col("s1.name").alias("supervisor_StartOfMonth"),
            col("s2.id").alias("supervisorId_EndOfMonth"),
            col("s2.name").alias("supervisor_EndOfMonth"),
            F.coalesce("s.locationId1", "s.locationId2").alias("locationId"),
            F.row_number().over(
                Window.partitionBy(F.coalesce("e.month", "s.month"), F.coalesce("e.email", "s.email"))
                .orderBy(col("e.dwh_valid_from").desc())
            ).alias("rn"),
        )
    )

    res_ids = [f"Res_AgentID{i}" for i in range(1, 9)]
    agent_base = (
        user_base_pre.alias("a")
        .join(reporting("contact_res_agent_link").alias("b"), col("a.id") == col("b.Contact_AgentID"), "left")
        .where(col("a.rn") == 1)
        .select(
            "a.month", col("a.id").alias("agentId"), "a.staffNumber", "a.name", "a.email", "a.hireDate",
            "a.businessUnitName", "a.Dept_StartofMonth", "a.Dept_EodMonth", "a.Title_StartofMonth",
            "a.Title_EodofMonth", *[f"b.{c}" for c in res_ids], "a.supervisor_EndOfMonth",
            "a.supervisorId_EndOfMonth", "a.supervisor_StartOfMonth", "a.supervisorId_StartOfMonth", "a.locationId",
        )
    )

    # HR state of each agent per month (matched by staff number, e-mail or name)
    hr_hist = _hr_records()
    a_staff = F.lpad(col("a.staffNumber"), 11, "0")
    a_email_local = _local_part(col("a.email"), minus_one=True)
    monthly_hr = (
        agent_base.alias("a")
        .join(
            hr_hist.alias("b"),
            ((a_staff == F.lpad(col("b.staffNumber"), 11, "0"))
             | (a_email_local == _local_part(col("b.workEmail"), minus_one=True))
             | (F.trim(F.lower(col("a.name"))) == F.trim(F.lower(col("b.name")))))
            & (col("a.month") >= col("b.dwh_valid_from"))
            & (col("a.month") < col("b.dwh_valid_to")),
            "left",
        )
        .select(
            "a.month", "a.name", "a.agentId", "a.staffNumber", "b.workersCompCodeState", "b.leaveDate",
            F.row_number().over(
                Window.partitionBy(a_staff, col("a.month"), a_email_local).orderBy(
                    a_staff, col("a.month"), col("b.dwh_valid_to").desc(), col("b.dwh_valid_from"),
                    F.when(col("a.email").like(COMPANY_EMAIL_DOMAIN), 0).otherwise(1).desc(),
                )
            ).alias("dateflag"),
        )
        .where(col("dateflag") == 1)
        .groupBy("month", "name", "agentId", "workersCompCodeState", F.lpad("staffNumber", 11, "0").alias("staffNumber"))
        .agg(F.max("leaveDate").alias("leaveDate"))
        .where(col("workersCompCodeState").isNotNull())
    )

    # last staff number the contact-centre platform knew for each agent
    last_version = (
        contact_raw("agents")
        .where((col("state") == "active") & col("staffNumber").isNotNull())
        .groupBy("id")
        .agg(F.max("dwh_valid_from").alias("Max_dwh_valid_From"))
    )
    last_staff_number = (
        last_version.alias("a")
        .join(contact_raw("agents").alias("b"),
              (col("a.id") == col("b.id")) & (col("a.Max_dwh_valid_From") == col("b.dwh_valid_from")))
        .select(
            "a.id", "a.Max_dwh_valid_From", "b.staffNumber",
            F.row_number().over(Window.partitionBy("a.id", "b.staffNumber").orderBy(col("a.Max_dwh_valid_From").desc()))
            .alias("mrn"),
        )
    )

    # leave date: prefer match by staff number (1), then e-mail (2), then name (3)
    hr_current = current(hr("hr_staff"))
    by_staff = (
        agent_base.alias("a")
        .join(monthly_hr.alias("b"), (col("a.agentId") == col("b.agentId")) & (col("a.month") == col("b.month")))
        .join(hr_current.alias("d"), F.lpad(col("b.staffNumber"), 11, "0") == F.lpad(col("d.staffNumber"), 11, "0"))
        .select("a.agentId", "a.month", "d.leaveDate", F.lit(1).alias("match_rank"))
    )
    by_email = (
        agent_base.alias("a")
        .join(hr_current.alias("d"),
              _local_part(col("a.email"), minus_one=False) == _local_part(col("d.workEmail"), minus_one=False))
        .select("a.agentId", "a.month", "d.leaveDate", F.lit(2).alias("match_rank"))
    )
    by_name = (
        agent_base.alias("a")
        .join(hr_current.alias("d"), F.lower(F.trim(col("a.name"))) == F.trim(F.lower(col("d.name"))))
        .select("a.agentId", "a.month", "d.leaveDate", F.lit(3).alias("match_rank"))
    )
    best_hr_match = (
        by_staff.unionByName(by_email).unionByName(by_name)
        .withColumn("mr", F.row_number().over(Window.partitionBy("agentId", "month").orderBy("match_rank")))
    )

    left_month = F.date_trunc("month", col("d.leaveDate"))
    return (
        agent_base.alias("a")
        .join(monthly_hr.alias("b"), (col("a.agentId") == col("b.agentId")) & (col("a.month") == col("b.month")), "left")
        .join(last_staff_number.alias("c"), (col("a.agentId") == col("c.id")) & (col("c.mrn") == 1), "left")
        .join(
            best_hr_match.alias("d"),
            (col("a.agentId") == col("d.agentId")) & (col("a.month") == col("d.month")) & (col("d.mr") == 1),
            "left",
        )
        .where(col("d.leaveDate").isNull() | (col("a.month") <= left_month))
        .select(
            "a.month", "a.name", "a.agentId",
            col("a.staffNumber").alias("staffNumber_CC"),
            col("c.staffNumber").alias("staffNumber"),
            "a.hireDate", "a.businessUnitName", "a.Dept_StartofMonth",
            F.coalesce("a.Dept_EodMonth", "a.Dept_StartofMonth").alias("Dept_EndofMonth"),
            "a.Title_StartofMonth",
            F.coalesce("a.Title_EodofMonth", "a.Title_StartofMonth").alias("Title_EndofMonth"),
            "a.email",
            col("d.leaveDate").alias("leftDate"),
            F.when(col("a.month") == left_month, "Termed").otherwise("active").alias("Status"),
            *[f"a.{c}" for c in res_ids],
            "a.supervisor_StartOfMonth",
            "a.supervisorId_StartOfMonth",
            F.coalesce("a.supervisor_EndOfMonth", "a.supervisor_StartOfMonth").alias("supervisor_EndOfMonth"),
            F.coalesce("a.supervisorId_EndOfMonth", "a.supervisorId_StartOfMonth").alias("supervisorId_EndOfMonth"),
            "a.locationId",
            "b.workersCompCodeState",
            F.row_number().over(
                Window.partitionBy(col("a.month"), F.coalesce(col("c.staffNumber"), col("a.agentId"))).orderBy(
                    col("a.hireDate").desc(), F.when(col("a.email").like(COMPANY_EMAIL_DOMAIN), 0).otherwise(1).desc()
                )
            ).alias("frk"),
        )
        .where(col("frk") == 1)
    )
