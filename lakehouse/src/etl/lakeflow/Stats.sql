-- Databricks notebook source
-- DBTITLE 1,PassengerCalls
CREATE LIVE TABLE PassengerCalls
AS 
With Queue as (
  Select
  ss.interactionId, ---- added 10/15/2023. interactionId field was originally not available in SessionSegments table when code was first created. But with the field being there now, adding it for consistency. No Impact to Result.
    ss.partyId,
    ss.callLegId,
    ss.queueId as QueueID,
    Q.name as QueueName,
    Q.description as QueueDescription,
    Q.businessUnitId,
    MIN(ss.segmentStart) AS SegmentStartDate,
    MAX(ss.segmentEnd) AS SegmentEndDate
  from
    ${gold_catalog}.${gold_schema}.ContactSegments ss
    left join ${raw_catalog}.${raw_main_schema}.res_queues Q on ss.queueId = Q.id  
    and ss.segmentStart >= Q.dwh_valid_from and ss.segmentStart  <  Q.dwh_valid_to --Modified 9/16/2022 due to new BusinessUnit added to Replace Home
  where
    queueId is Not null
  group by
  ss.interactionId,
    ss.partyId,
    ss.callLegId,
    ss.queueId,
    Q.name,
    Q.description,
    Q.businessUnitId
),

Skills as (
  SELECT
    interactionId,
    callLegId,
    partyId,
    array_join(collect_set(b.name), ';') as SkillName
  FROM
    ${gold_catalog}.${gold_schema}.ContactSkillAssignments a
    left join ${raw_catalog}.${raw_main_schema}.agent_skills b on a.skillId = b.id and b.dwh_current_flag = true and b.dwh_delete_flag = false
  group by
    interactionId,
    callLegId,
    partyId
),

MgrName as ( Select
      distinct name,
      id
    from
      ${raw_catalog}.${raw_main_schema}.agents
    WHERE
      dwh_current_flag = true),

DisconnectType as ( 
Select interactionId, partyId, callLegId, Max(segmentend) as MaxEnd
 from ${gold_catalog}.${gold_schema}.ContactSegments
where disconnectType is not null
-- and disconnectType <> 'error'
group by interactionId, partyId, callLegId
),

DisconnectType_Final as (
Select Distinct a.interactionId, a.callLegId, a.partyId, a.disconnectType from ${gold_catalog}.${gold_schema}.ContactSegments a 
join DisconnectType b on a.interactionId = b.interactionId and a.callLegId = b.callLegId and a.partyId = b.partyId and a.segmentEnd = b.MaxEnd
where disconnectType is not null
 --and disconnectType <> 'error'
)


SELECT
  from_utc_timestamp(sm.eventTimestamp, 'PST') AS EventDateTime_LAX,
  from_utc_timestamp(sm.eventTimestamp, 'Australia/Sydney') AS EventDateTime_SYD,
  from_utc_timestamp(sm.eventTimestamp, 'Europe/London') AS EventDateTime_LHR,
  cast(sm.eventTimestamp as timestamp) as EventDateTime_UTC,
  case
    when a.direction = 'inbound' then 1
    when a.direction = 'outbound' then 2
    else '3'
  end as DirectionCode,
  a.interactionId,
  a.callLegId,
  a.partyId,
  sm.value,
  sm.name as Metric,
  left(replace(a.ani, 'tel:+', ''), 100) as CallerNumber,
  left(replace(a.dnis, 'tel:+', ''), 100) as DialedNumber,
  pp.purpose as PurposeCode,
  pp.agentId as agentId,
  us.staffNumber,
  us.name as AgentName,
  Que.QueueID,
  Que.QueueName,
  'NULL' AS QueueDescription,
  Skill.SkillName as SkillName,
  queDiv.name As BusinessUnit, 
  usDiv.name AS AgentBusinessUnit,
  a.routingRing,
  a.agentBullseyeRing,
  usm.name AS SupervisorName,
  us.hireDate,
  us.department,
  us.title,
  a.usedRouting,
  a.mediaType, -- added 08/10/2022
  a.remote -- added 02/21/2023
  ,df.disconnectType
FROM
  ${gold_catalog}.${gold_schema}.ContactSessions a
  join ${gold_catalog}.${gold_schema}.ContactSessionMetrics sm on a.callLegId = sm.callLegId   and a.partyId = sm.partyId   and a.interactionId = sm.interactionId
  join ${gold_catalog}.${gold_schema}.ContactParticipants pp on a.partyId = pp.partyId   and a.interactionId = pp.interactionId
  left join ${raw_catalog}.${raw_main_schema}.agents us on pp.agentId = us.id   and sm.eventTimestamp >= us.dwh_valid_from   and sm.eventTimestamp <=  us.dwh_valid_to
  left join MgrName usm on us.supervisorId = usm.id
  left join Queue Que on   sm.callLegId = Que.callLegId   and sm.partyId = Que.partyId  and sm.interactionId = Que.interactionId and sm.eventTimestamp between Que.SegmentStartDate  and Que.SegmentEndDate
  left join Skills Skill on sm.callLegId = Skill.callLegId and sm.partyId = Skill.partyId and sm.interactionId = Skill.interactionId
  left join ${raw_catalog}.${raw_main_schema}.business_units AS queDiv ON queDiv.id = Que.businessUnitId 
												--and queDiv.name in ('Home', 'AU', 'UK')   --Modified 9/16/2022 due to new BusinessUnit added to Replace Home
												and queDiv.dwh_current_flag = true
                        and queDiv.dwh_delete_flag = false
  left join ${raw_catalog}.${raw_main_schema}.business_units AS usDiv ON usDiv.id = us.businessUnitId 
												--and usDiv.name in ('Home', 'AU', 'UK')  --Modified 9/16/2022 due to new BusinessUnit added to Replace Home
												and usDiv.dwh_current_flag = true
                        and usDiv.dwh_delete_flag = false

   Left join DisconnectType_Final df on a.interactionId = df.interactionId and a.callLegId = df.callLegId and a.partyId = df.partyId                     
where 1=1
  and cast(from_utc_timestamp(sm.eventTimestamp, 'PST') as Date) >= '2021-12-10'
  ----order by EventDateTime_LAX desc


-- COMMAND ----------

-- DBTITLE 1,PassengerCallStats
CREATE LIVE TABLE PassengerCallStats
AS
With
Skills as (
  SELECT
    distinct interactionId,
    callLegId,
    partyId,
    skillId,
    b.name as SkillName
  FROM
    ${gold_catalog}.${gold_schema}.ContactSkillAssignments a
    left join ${raw_catalog}.${raw_main_schema}.agent_skills b on a.skillId = b.id
  where
    b.name in (
      'Consumer',
      'Travel Agent',
      'US - Round the World',
      'US - Irregular Ops'
    )
),
PassengerCalls_Sum AS (
  Select
    DirectionCode,
    interactionId,
    callLegId,
    partyId,
    usedRouting,
    routingRing,
    agentBullseyeRing,
    AgentName,
    PurposeCode,
    agentId,
    staffNumber,
    BusinessUnit,
    CallerNumber,
    DialedNumber,
    QueueName,
    QueueId,
	SkillName as SkillName_Combined,
	department,
	title,
	hireDate,
	mediaType,----Added mediaType 02/21/2023 to distingush between new Record Types (Voice, Message and Chat) that was recently implemented.
	    Min(EventDateTime_LAX) as StartDateTime,
    Min(EventDateTime_UTC) as EventDateTime_UTC,
    sum(
      case
        when Metric = 'nOffered' then 1
        else null
      end
    ) as Offered,
    Max(
      case
        when Metric = 'tAnswered' then Value / 1000
        else null
      end
    ) as Answered_Second,
    Max(
      case
        when Metric = 'tAbandon' then Value / 1000
        else null
      end
    ) as Abandoned_Second,
    Max(
      case
        when Metric = 'tHandle' then Value / 1000
        else null
      end
    ) as Handled_Second,
	Max(
      case
        when Metric = 'tShortAbandon' then Value / 1000
        else null
      end
    ) as ShortAbandon_Second  ---- Added 03/16/2023 to factor for Short Abandons    
  from
    live.PassengerCalls
    where DirectionCode = 1 	--added 08/10/2022
  Group by
    DirectionCode,
    interactionId,
    callLegId,
    partyId,
    usedRouting,
    routingRing,
    agentBullseyeRing,
    AgentName,
    PurposeCode,
    agentId,
    staffNumber,
    BusinessUnit,
    CallerNumber,
    DialedNumber,
    QueueName,
    QueueId,
	SkillName, 
	department,
	title,
	hireDate,
	mediaType
		
),
Calls_OverSLA as
(Select distinct interactionId, callLegId, partyId from live.PassengerCalls
where
Metric = 'nOverSla'),
CallOutcome_Code  as
(SELECT   Distinct  interactionId, partyId,  callLegId, queueId, callOutcomeCode, b.name as CallOutcomeName
  FROM ${gold_catalog}.${gold_schema}.ContactSegments a 
  join  ${raw_catalog}.${raw_main_schema}.call_outcome_codes b on a.callOutcomeCode = b.id
  where callOutcomeCode is not null and dwh_current_flag = true and b.dwh_delete_flag = false
  ),
CallsAnwered_Abandoned
AS (
Select
FROM_UNIXTIME((UNIX_TIMESTAMP(StartDateTime) div 900) * 900) AS IntervalStart
,FROM_UNIXTIME((UNIX_TIMESTAMP(EventDateTime_UTC) div 900) * 900) AS IntervalStart_UTC
,FROM_UNIXTIME(UNIX_TIMESTAMP(a.StartDateTime)-COALESCE(Answered_Second, Abandoned_Second,0)) as OfferDateTime --added 08/10/2022 
,FROM_UNIXTIME(UNIX_TIMESTAMP(a.EventDateTime_UTC)-COALESCE(Answered_Second, Abandoned_Second,0)) as OfferDateTime_UTC --added 08/10/2022 
,a.StartDateTime
,a.EventDateTime_UTC
,a.DirectionCode
,a.interactionId
,a.callLegId
,a.partyId
,a.usedRouting
,a.routingRing
,a.agentBullseyeRing
,a.AgentName
,a.PurposeCode
,a.agentId
,a.staffNumber
,a.CallerNumber
,a.DialedNumber
,a.QueueName
,b.SkillName
,a.Offered
,a.Answered_Second
,case when c.interactionId is not null then 1 else 0 end as Answered_OverSLA
,a.Abandoned_Second
,a.Handled_Second
,a.BusinessUnit  
,d.CallOutcomeName
,a.SkillName_Combined
,a.department
,a.title
,a.hireDate
,a.mediaType
,a.ShortAbandon_Second ---- Added 03/16/2023 to factor for Short Abandons

from PassengerCalls_Sum a
left join Skills b on a.interactionId = b.interactionId and a.callLegId = b.callLegId and a.partyId = b.partyId ----and a.agentId = b.agentId
left join Calls_OverSLA c on a.interactionId = c.interactionId and a.callLegId = c.callLegId and a.partyId = c.partyId
left join CallOutcome_Code d on a.interactionId = d.interactionId and a.callLegId = d.callLegId and a.partyId = d.partyId and a.QueueId = d.queueId
where DirectionCode = 1 and (Answered_Second is not null or Abandoned_Second is not null)
),
Final as (
Select
StartDateTime
,a.hireDate
,IntervalStart
,IntervalStart_UTC
,cast(StartDateTime as Date) as Date
,cast(EventDateTime_UTC as Date) as Date_UTC
,QueueName
,SkillName
,case when Answered_Second is not null then 1 else 0 end as AnsweredCount
,COALESCE(Answered_Second,0) as AnsweredTimeTotal
,case when Abandoned_Second is not null then 1 else 0 end as AbandonedCount
,COALESCE(Abandoned_Second,0) as AbandonedTimeTotal
,COALESCE(Handled_Second,0) as HandledTimeTotal
,COALESCE(Answered_OverSLA,0) as Answered_OverSLA
,Case when (QueueName = 'Sales' and SkillName = 'Consumer') or (QueueName = 'Sales' and SkillName is null )  or (QueueName in ('Sales', 'US - Internal Direct Sales - 2117')) then 'Direct Sales'
when (QueueName = 'Sales' and SkillName = 'Travel Agent')  or QueueName = 'US - Internal TA Sales - 2118' or (QueueName = 'Sales - TA' and (SkillName = 'Travel Agent' or SkillName is null)) then 'TA Sales'
when (QueueName = 'Service' and SkillName in ('Consumer')) or (QueueName = 'Service' and SkillName is null)  or QueueName = 'US - Internal Service Direct 3083' then 'Service'
when (QueueName = 'Service' and SkillName = 'Travel Agent')  or QueueName = 'US - Internal Service TA 3084' then 'Service'
when (QueueName = 'Service' and SkillName in ( 'US - Round the World', 'Irregular Ops')) then 'Service'
when (QueueName = 'Round the World') then 'Service'
when QueueName in ('Payment' ,'US - Internal Existing Groups 1260', 'Round the World', 'Charter Flights')  then 'Service'
When QueueName in ('Customer Relations', 'US - Internal Customer Relations', 'Customer Relations - Covid19') then 'Customer Relations'
when QueueName in ('Air Customer Service', 'US - Internal Air - 1206', 'US - Air VIP')  then 'Air Customer Service'
when QueueName in ('Enroute', 'US - Internal Enroute 1208')  then 'Air Operations'
When QueueName in ('US - Air Resolutions')  then 'Air Resolution'
When QueueName in ('Air Back Office')  then 'Air Back Office'
when QueueName in ('Inside Sales') then 'Inside Sales'
when QueueName in ('New Groups') then 'Group Sales'
when QueueName in ('Passenger Relations') then 'Passenger Services'
when QueueName in ('WFM - 1501, 1502') then 'WFM'
when QueueName in ('Chat') then 'Chat'
when QueueName in ('Outbound') then 'Outbound Sales'
when QueueName in ('Operator') then 'Front Desk'
--when QueueName in ('AU - Air') then 'Air Customer Service'
--when QueueName in ('AU - Customer Relations') then 'Customer Relations'
--when QueueName in ('AU - Sales', 'AU - transfer Sales Consumer', 'AU - transfer Sales TA') then 'Reservations Sales'
--when QueueName in ('AU - Service', 'AU - transfer Service Consumer', 'AU - transfer Service TA') then 'Reservations Service'
--when QueueName in ('UK - Air', 'UK- Internal Air') then 'Air Customer Service'
--when QueueName in ('UK - Passenger Services', 'UK - Internal Pre Departure') then 'Passenger Services'
--when QueueName in ('UK - Internal Post Departure') then 'Customer Relations'
--when QueueName in ('UK - Reservations', 'UK - Internal Reservations') then 'Reservations'
--Else QueueName + '-as is' 
Else QueueName
end as DeptGroup
,a.agentId
,a.usedRouting
,a.routingRing
,a.agentBullseyeRing
,a.AgentName
,a.staffNumber
,a.interactionId
,a.DirectionCode
,case when routingring > 1 and a.agentBullseyeRing > 1 then 1 else 0 end as OverFlowCount
,case when a.department = 'Direct Sales Remote' then 'Direct Sales' else a.department end as Department_OF_Taken 
,a.title as Title_OF_Taken
,CallOutcomeName
,a.SkillName_Combined
,FROM_UNIXTIME((UNIX_TIMESTAMP(OfferDateTime) div 900) * 900) AS Offered_Interval --added 08/10/2022
,FROM_UNIXTIME((UNIX_TIMESTAMP(OfferDateTime_UTC) div 900) * 900) AS Offered_Interval_UTC --added 08/10/2022
,mediaType
,BusinessUnit ----Added mediaType 02/28/2023 to distingush between new Record Types (Voice, Message and Chat) that was recently implemented.
,case when ShortAbandon_Second is not null then 1 else 0 end as ShortAbandonedCount ---- Added 03/16/2023 to factor for Short Abandons
,COALESCE(ShortAbandon_Second,0) as ShortAbandonedTimeTotal							---- Added 03/16/2023 to factor for Short Abandons

from CallsAnwered_Abandoned a
--left join main.agents b on a.staffNumber = b.staffNumber and a.EventDateTime_UTC between b.dwh_valid_from and b.dwh_valid_to
)
Select
cast(IntervalStart as timestamp) AS IntervalStart
,cast(IntervalStart_UTC as timestamp) AS IntervalStart_UTC
,cast(StartDateTime as Date) as Date
,QueueName
,SkillName
,Sum(COALESCE(AnsweredCount,0)) as AnsweredCount
,Sum(COALESCE(AnsweredTimeTotal,0)) as AnsweredTimeTotal
,Sum(COALESCE(AbandonedCount,0)) as AbandonedCount
,Sum(COALESCE(AbandonedTimeTotal,0)) as AbandonedTimeTotal
,Sum(COALESCE( HandledTimeTotal,0)) as HandledTimeTotal
,Sum(COALESCE( Answered_OverSLA,0)) as Answered_OverSLA
,DeptGroup
,DirectionCode
,Sum(COALESCE(OverFlowCount,0)) as OverFlowCount
,Department_OF_Taken
,Title_OF_Taken
,usedRouting
,routingRing
,agentBullseyeRing
,CallOutcomeName
,SkillName_Combined
,cast(Offered_Interval as timestamp) as Offered_Interval
,cast(Offered_Interval_UTC as timestamp) as Offered_Interval_UTC
,mediaType
,BusinessUnit ----Added mediaType 02/28/2023 to distingush between new Record Types (Voice, Message and Chat) that was recently implemented.
,Sum(COALESCE(ShortAbandonedCount,0)) as ShortAbandonedCount				---- Added 03/16/2023 to factor for Short Abandons
,Sum(COALESCE(ShortAbandonedTimeTotal,0)) as ShortAbandonedTimeTotal		---- Added 03/16/2023 to factor for Short Abandons

from Final
group by
IntervalStart
,IntervalStart_UTC
,cast(StartDateTime as Date)
,QueueName
,SkillName
,DeptGroup
,DirectionCode
,Department_OF_Taken
,Title_OF_Taken
,usedRouting
,routingRing
,agentBullseyeRing
,CallOutcomeName
,SkillName_Combined
,cast(Offered_Interval as timestamp) 
,cast(Offered_Interval_UTC as timestamp)
,mediaType
,BusinessUnit ----Added mediaType 02/28/2023 to distingush between new Record Types (Voice, Message and Chat) that was recently implemented.

-- COMMAND ----------

-- DBTITLE 1,CampaignCalls
-- Please see below for updated Code: 

------Updated code to pull back all 'Campaign Designated Number'
------Will only show the first call in event of transfers
------Will show all Call Segments ie, Answered, abandoned, and abandoned before queue
------Will have contact-centre Data only. Discontiuned Avaya IQ/CMS

/*Code runs the campaign phone numbers to check against calls and bookings to determine if the campaign number is link to a call and Booking.*
Contacts:
For Code Change or Update: Xin Pan, Kamaljeet Dhah,Xichun Han
Databrick code replacement: Nischay Gowda
Business user in marketing: Tania Dawood
*/


----------Temp Table to set designated Campaign Phone numbers and date range in which it was effective------------

CREATE LIVE TABLE CampaignCalls 
as
/*With
NumberList as 
(
select '1900-01-01' as StartDate, '2022-09-29' as EndDate, '18003049616' as DialedNumber_List
Union
select '2023-10-30', '2099-01-01', '18003049616'
Union
select '1900-01-01', '2022-09-29', '18669845464'
Union
select '2023-10-30', '2099-01-01', '18669845464'
Union
select '1900-01-01', '2022-09-29', '18003814596'
Union
select '2023-10-30', '2099-01-01', '18003814596'
Union
select '1900-01-01', '2022-09-29', '18002077286'
Union
select '2023-10-30', '2099-01-01', '18002077286'
Union
select '1900-01-01', '2022-09-29', '18557074837'
Union
select '2023-10-30', '2099-01-01', '18557074837'
Union
select '1900-01-01', '2022-09-29', '18882078824'
Union
select '2023-10-30', '2099-01-01', '18882078824'
Union
select '1900-01-01', '2022-09-29', '18882320013'
Union
select '2023-10-30', '2099-01-01', '18882320013'
Union
select '1900-01-01', '2022-09-29', '18002143110'
Union
select '2023-10-30', '2099-01-01', '18002143110'
Union
select '1900-01-01', '2022-09-29', '18882524530'
Union
select '1900-01-01', '2022-09-29', '18559184546'
Union
select '1900-01-01', '2022-09-29', '18557162326'
Union
select '1900-01-01', '2022-09-29', '18557962326'
Union
select '1900-01-01', '2022-09-29', '18553384546'
Union
select '1900-01-01', '2022-09-29', '18554774837'
Union
select '1900-01-01', '2022-09-29', '18005105461'
Union
select '1900-01-01', '2022-09-29', '18882078826'
Union
select '1900-01-01', '2022-09-29', '18882329421'
Union
select '1900-01-01', '2022-09-29', '18003049651'
Union
select '1900-01-01', '2022-09-29', '18006335793'
Union
select '1900-01-01', '2022-09-29', '18004643595'
Union
select '2022-09-29', '2099-01-01', '18449215100'

--Added 07/31/2024 with Start Date 07/10/2024 to 
Union
select '2024-07-15', '2099-01-01', '18666957131' -- Abandoncart modal control
Union
select '2024-07-15', '2099-01-01', '18773164536' -- Abandoncart modal variant
Union
select '2024-07-10', '2099-01-01', '18553003950' -- River Control
Union
select '2024-07-10', '2099-01-01', '18773055222' -- River Varaint 
Union
select '2024-07-10', '2099-01-01', '18557462326' -- Ocean Control
Union
select '2024-07-10', '2099-01-01', '18773064536' -- Ocean Variant
Union
select '2024-07-10', '2099-01-01', '18558387673' -- Expedition Control
Union
select '2024-07-10', '2099-01-01', '18772222493' -- Expedition Variant
)
,
*/

with
NetFareRevenue_Base as
 (
 select
        dp.TravelPnr  as TravelPnr
       ,dp.BookingDateTime as BookingDateTime
       ,SUM(case when da.fpaAccountGroup in ('Gross') then n.AmountUSD else 0 end) as GrossRevenue
       ,SUM(case when da.fpaAccountGroup in ('Discount') then n.AmountUSD else 0 end)*(-1) as Discount
       ,SUM(case when da.fpaAccountGroup in ('Voucher') then n.AmountUSD  else 0 end)*(-1) as Voucher
       ,SUM(case when da.fpaAccountGroup in ('Voucher redemption') then n.AmountUSD else 0 end) as Voucher_redemption
       ,SUM(case when da.fpaAccountGroup in ('Gross') then n.AmountLocalCcy else 0 end) as Grossrevenue_LC
       ,SUM(case when da.fpaAccountGroup in ('Discount') then n.AmountLocalCcy else 0 end)*(-1) as Discount_LC 
       ,SUM(case when da.fpaAccountGroup in ('Voucher') then n.AmountLocalCcy else 0 end)*(-1) as Voucher_LC
       ,SUM(case when da.fpaAccountGroup in ('Voucher redemption') then n.AmountLocalCcy else 0 end) as Voucher_redemption_LC
from
 ${gold_catalog}.${gold_ncf_schema}.NetFareRevenue n
left join ${gold_catalog}.${gold_maindata_schema}.DimPassenger dp on n.JourneyPaxKey = dp.JourneyPaxKey
left join ${gold_catalog}.${gold_maindata_schema}.DimGLAccount da ON n.AccountKey =da.AccountKey
left join ${gold_catalog}.${gold_maindata_schema}.DimFareProduct dpt on n.ProductKey=dpt.ProductKey
where 1 = 1
and
 da.AccountGroup not in
('Cost', 'Adjustment', 'Commission', 'Voucher redemption')
and dpt.ProductCategory not in ('CancelRevenue')
and (dpt.IsGiftExpensed = false or dpt.IsGiftExpensed is null)
and (dp.IsPaxCancelled <> 1 or da.AccountType <> 'Inflight gifts')
group by
 dp.TravelPnr,
 dp.BookingDateTime
)
,
NetFareRevenue as
(
  select
      TravelPnr,
      BookingDateTime,
      GrossRevenue-Discount-Voucher-Voucher_redemption as NetFareRevenue_USD,
      Grossrevenue_LC-Discount_LC-Voucher_LC-Voucher_redemption_LC as NetFareRevenue_LC
 from NetFareRevenue_Base
)

----------Pull in all US base Calls and the call start time------------
,
Mintime as (

select Min(EventDateTime_LAX) as Min_CallDateTime
,Max(EventDateTime_LAX) as CallEndTime
,interactionId
,CallerNumber
,DialedNumber
from 
live.PassengerCalls a
--Reporting.[dm_callcenter].[PassengerCalls]
--join NumberList b on a.DialedNumber = b.DialedNumber_List and a.EventDateTime_LAX >= b.StartDate and a.EventDateTime_LAX < b.EndDate
join ${gold_catalog}.${gold_schema}.CampaignPhoneNumbers b on a.DialedNumber = b.phone_number and b.active_flag=1
where (BusinessUnit in ( 'Home', 'US Inbound Queues') or BusinessUnit is null)
/*
and (
         -- Added numbers by product on 10/30/2023
         DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8003049616%' end  -- River US
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8669845464%' end  -- Ocean US
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8003814596%' end  -- Exp US
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8002077286%' end  -- River CA
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8557074837%' end  -- Ocean CA
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8882078824%' end  -- OLB US
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8882320013%' end  -- OLB CA
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' and EventDateTime_LAX < '2023-10-30' then 'DONOTCOUNT' else  '%8002143110%' end  -- Exp CA
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8882524530%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8559184546%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8557162326%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8557962326%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8553384546%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8554774837%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8005105461%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8882078826%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8882329421%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8003049651%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8006335793%' end
      or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then 'DONOTCOUNT' else  '%8004643595%' end -- Added these 2 numbers below on 8/22/2022
	    or DialedNumber like case when EventDateTime_LAX >= '2022-09-29' then '%8449215100%' else  'DONOTCOUNT' end -- Added for New Campaign Number
      or DialedNumber like case when EventDateTime_LAX >= '2024-02-05' then '%8003049640%' else  'DONOTCOUNT' end -- Added for New Campaign Number on 2/2/2024 - vrc
      or DialedNumber like case when EventDateTime_LAX >= '2024-02-05' then '%8007855740%' else  'DONOTCOUNT' end -- Added for New Campaign Number on 2/2/2024 - voc
      or DialedNumber like case when EventDateTime_LAX >= '2024-02-05' then '%8007855996%' else  'DONOTCOUNT' end -- Added for New Campaign Number on 2/2/2024 - vxp
    ) */
	and DirectionCode = 1
group by interactionId, CallerNumber, DialedNumber
)
--Select * from Mintime where interactionId = 'a56889bf-b372-425b-8141-5bfe0fe0e896'
,

----------Join back to call table to determine rather call was answered or abandoned----------
CampaignCallBase as (
Select a.interactionId, b.department, b.title, b.SkillName, a.Min_CallDateTime, a.CallEndTime, COALESCE (b.EventDateTime_LAX, a.Min_CallDateTime) as MetricSegmentTime,  b.callLegId, b.partyId,   b.value
,case when b.Metric is null then 'Abandoned_BeforeQueue'
       when b.Metric ='tAbandon' then 'Abandoned_InQueue'
       when b.Metric ='tAnswered' then 'Answered'
       end as Metric
,COALESCE (b.CallerNumber, a.CallerNumber) as CallerNumber, COALESCE (b.DialedNumber, a.DialedNumber) as DialedNumber, b.QueueName, b.AgentName,  b.agentId
,COALESCE (b.interactionId, a.interactionId) as interactionId_B
,ROW_NUMBER() over(partition by a.interactionId order by b.interactionId, b.EventDateTime_LAX) as RowOrder 
from Mintime a 
Left join live.PassengerCalls b on a.interactionId = b.interactionId and  b.Metric in ('tAnswered', 'tAbandon') and b.DirectionCode = 1 and Len(b.DialedNumber) = 11
--order by a.interactionId, b.EventDateTime_LAX
)
  
--select * from CampaignCallBase_Pre
--where  CallerNumber  like '%7577185615%'
--order by MetricSegmentTime
,

--------- Bookingbase -----
bookingsbase as (
Select 
a.pnr
,a.pnrCreatedDate as BookingDate
,b.office
,b.DepositAmt+b.BalanceAmt+b.AgencyCommission as GrossRevenue
,b.CurrencyCode
,b.PromoCode
,case
      when Upper(b.Status) = 'K' then 'CANCELLED'
      when (
        b.Status = ''
        and b.ReceivedAmt > 0
      )
      or Upper(b.ConfirmFlag) = 'Y' then 'CONFIRMED'
      when b.Status = ''
      and b.ReceivedAmt = 0 then 'OPTION'
      else 'Other'
    end as BookingStatus
,b.BookingChannel
,b.Status
,b.ConfirmFlag
,b.ReceivedAmt
,c.assignedAgentId as BookingAgentId
,concat(e.firstName,' ',e.lastName) as AgentFullName
,b.FlightNumber as FlightCode
,d.RouteName as Itinerary
,d.DepartureDate as fromdate
,f.username
,f.department as DepartmentName
,f.title as DepartmentTitle
,f.supervisorId
,f.id as agentid_booking
,g.name as ManagerName
,j.NetFareRevenue_USD 
--,d.supermkt as product
,b.DirectFlag
,row_number()over(partition by a.pnr order by f.dwh_valid_from desc) as rn
from ${raw_catalog}.${raw_sup_schema}.pnr_header a
left join ${raw_catalog}.${raw_sup_schema}.pnr_sales  b on a.pnr = b.pnr and b.dwh_current_flag  = 'true' and b.dwh_delete_flag = 'false' 
left join ${raw_catalog}.${raw_sup_schema}.pnr_agent_assignments c on a.pnr = c.pnr and c.dwh_current_flag = 'true' and c.dwh_delete_flag = 'false'
left join ${raw_catalog}.${raw_sup_schema}.flights  d on b.FlightNumber = d.FlightNumber and d.dwh_current_flag = 'true' and d.dwh_delete_flag = 'false'
left join  (Select distinct agentId, firstName, lastName, email from ${raw_catalog}.${raw_sup_schema}.res_agents where dwh_current_flag = 'true') e on c.assignedAgentId = e.agentId 
left join  ${raw_catalog}.${raw_main_schema}.agents as f on   substring_index(lower(e.email), '@', 1)=substring_index(lower(f.email), '@', 1) and
a.pnrCreatedDate>=from_utc_timestamp(f.dwh_valid_from,'America/Los_Angeles') and a.pnrCreatedDate<from_utc_timestamp(f.dwh_valid_to,'America/Los_Angeles')
and f.state = 'active' and (lower(f.title) not like '%test%' or lower(f.department) not like '%test%' or lower(f.name) not like '%test%') --and f.dwh_delete_flag = 'false'
left join  (Select distinct id, name from ${raw_catalog}.${raw_main_schema}.agents where state = 'active' and dwh_current_flag = 'true') as g on f.supervisorId = g.id 
left join NetFareRevenue j on a.pnr = j.TravelPnr

where 
--a.pnr = 7993185 and 
a.dwh_current_flag= 'true' and a.dwh_delete_flag = 'false'
and cast(pnrCreatedDate as date)>='2021-01-01'----Be sure to update Date
) 
,
bookingsbase2 as (
select a.*,b.ffpNumber from bookingsbase a
left join ${raw_catalog}.${raw_sup_schema}.pnr_passengers b on a.pnr = b.pnr and b.dwh_current_flag = 'true' and b.dwh_delete_flag = 'false'
union
select a.*,b.ffpNumber from bookingsbase a
left join ${raw_catalog}.${raw_sup_schema}.pnr_cancelled_passengers b on a.pnr = b.pnr and b.dwh_current_flag = 'true' and b.dwh_delete_flag = 'false'
)
,
----------Filter Call to only return the first session of the call----------
AllCallBase as (
  Select
   a.interactionId
  ,a.department as org_department
  , case
  when (a.department = 'Direct Sales' and a.title like'%Consultant%') OR a.title = 'Travel Sales Consultant'then 'Direct Sales'
  when (a.department = 'TA Sales' and a.title like'%Consultant%') OR a.title = 'Travel Agent Sales Consultant'then 'TA Sales'
  when (a.department = 'Outbound'and a.title like'%Consultant%') OR a.title = 'Outbound Sales Consultant'then 'Outbound Sales'
  when (a.department in ('Chat', 'Chat Sales') and a.title like '%Consultant%') OR a.title = 'Web Chat Sales Consultant'then 'Chat Sales'
  else a.department
  end as department
  ,a.title
  ,a.SkillName
  ,a.Min_CallDateTime
  ,a.Metric 
    ,MetricSegmentTime as CallStart,
		CallEndTime,
    a.CallerNumber as CallerNumber,
    a.interactionId as CallId,
    a.DialedNumber as DialedNumber,
    a.QueueName as QueueName,
    a.agentId,
    case
      when a.QueueName in (
        'Sales',
        'US - Internal Direct Sales - 2117',
        'Sales - TA',
        'US - Internal TA Sales - 2118'
      ) then 'Res-Sales'
      when a.QueueName in (
        'Service',
        'US - Internal Service Direct 3083',
        'US - Internal Service TA 3084',
        'Round the World',
        'Payment',
        'US - Internal Existing Groups 1260',
        'Round the World',
        'Charter Flights'
      ) then 'Res-Service'
      else 'Other'
    end as Dept
  from
    CampaignCallBase a
    left join (
      Select
        Max(cast(Value as float) / 1000) as HandleSeconds,
        interactionId,
        callLegId,
        partyId,
        AgentName,
        QueueName
      From
        live.PassengerCalls
      group by
        interactionId,
        callLegId,
        partyId,
        AgentName,
        QueueName
    ) b on a.interactionId = b.interactionId
    and a.callLegId = b.callLegId
    and a.partyId = b.partyId
    and a.QueueName = b.QueueName ----and a.AgentName = b.AgentName
    where a.RowOrder = 1
)

--Select * from AllCallBase


--AllCallBase_CMS as (
--  select
--    DateTime as CallStart,
--    from_unixtime(to_unix_timestamp(DateTime) + duration) AS CallEndTime,
--    Right(replace(replace(calling_pty, ' ', ''), '.', ''), 10) as CallerNumber,
--    callid as CallID,
--    d.ANI as DialedNumber,
--    b.QueueName,
--    c.Dept
--  from
--    raw_avayacms.Call_rec a
--    join raw_avaya.Queues b on a.dispsplit = b.QueueID
--    JOin raw_laxsql2_epic.CallCategories c on b.EpicID = c.group_id
--    left join raw_avaya.ANI d on a.firstvdn = d.VDN
--  where
--    firstvdn like '%1822%'
--    and dispsplit is not null
--    and disposition = 2
--    and row_date < '2021-12-10'
--),
--AllCallBase as (
--  select
--    *
--  from
--    AllCallBase_Gen
--  UNION
--  select
--    *
--  from
--    AllCallBase_CMS
------------------------------------------

--),
/*
----------Check the Caller Phone number to link it to a Loyalty Member ID Phone 1----------
LoyaltyID_Phone1 as (
  Select
    *
  from
    AllCallBase a
    left join (
      SELECT
        Right(Replace(telephone1, ' ', ''), 10) as telephone1,
        Right(Replace(telephone2, ' ', ''), 10) as telephone2,
        loyalty_member_id
      FROM
        ${raw_catalog}.${d365crm_schema}.passenger_contacts
      where
        dwh_current_flag = 1
        and dwh_delete_flag = 0 --and telephone1 is not null
        and len(telephone1) >=10
    ) b on Right(a.CallerNumber,10) = Right(Replace(telephone1, ' ', ''), 10)
),

----------Check the Caller Phone number to link it to a Loyalty Member ID Phone 2----------
LoyaltyID_Phone2 as (
  Select
    *
  from
    AllCallBase a
    left join (
      SELECT
        Right(Replace(telephone1, ' ', ''), 10) as telephone1,
        Right(Replace(telephone2, ' ', ''), 10) as telephone2,
        loyalty_member_id
      FROM
       ${raw_catalog}.${d365crm_schema}.passenger_contacts
      where
        dwh_current_flag = 1
        and dwh_delete_flag = 0 --and telephone1 is not null
        and len(telephone2) >=10
    ) b on Right(a.CallerNumber,10) = Right(Replace(telephone2, ' ', ''), 10) ---or  a.CallerNumber COLLATE Latin1_General_CI_AS = Right(Replace(telephone2, ' ', ''),10)
),

----------Take Loyalty ID return from join to Phone number and pull the FFP number to be use to find any bookings
LoyaltyID as (
  Select
    *
  from
    LoyaltyID_Phone1
  UNION
Select
    *
  from
    LoyaltyID_Phone2
),

CRM_Loyalty_FFP as (
Select 
a.interactionId
,a.Min_CallDateTime
,a.Metric
,a.CallStart
,a.CallEndTime
,a.CallerNumber
,a.CallId
,a.DialedNumber
,a.QueueName
,a.Dept
,a.telephone1
,a.telephone2
,a.loyalty_member_id
,b.ffpNumber
 from LoyaltyID a 
left join ${raw_catalog}.${raw_sup_schema}.loyalty_members b on a.loyalty_member_id = b.contactid and b.dwh_current_flag = true and b.dwh_delete_flag = false
),

-------------Added 09-04-2024 Contact FFP number from CRM Prod, due to discontinuation of CRM and LoyaltyMP table.
CRM_Prod_FFP_Phone as (
select distinct a.ffp_number,   a.address1_telephone1
 from ${raw_catalog}.${raw_crm_schema}.passenger_contacts  a 
--  join ${raw_catalog}.${raw_crm_schema}.communicationinformation  b on a.contactid = b.vkg_contact and b.dwh_current_flag = true and b.dwh_delete_flag = false and Len(b.vkg_phonenumber)>=10
 where a.dwh_current_flag = true and a.dwh_delete_flag = false
 and len(ffp_number)>1 
  and Len(a.address1_telephone1)>=10
 --order by b.vkg_phonenumber
),


CombinedCRM_LoyaltyID_Phone_FFP as (
Select 
Distinct
a.interactionId
,a.Min_CallDateTime
,a.Metric
,a.CallStart
,a.CallEndTime
,a.CallerNumber
,a.CallId
,a.DialedNumber
,a.QueueName
,a.Dept
--,a.loyalty_member_id
,b.ffp_number as ffpNumber
from LoyaltyID a 
join CRM_Prod_FFP_Phone b on Right(a.CallerNumber,10) = Right(Replace(address1_telephone1, ' ', ''), 10)


UNION

Select  
distinct 
interactionId
,Min_callDateTime  
,a.Metric
,a.CallStart
,a.CallEndTime
,a.CallerNumber
,a.CallID
,a.DialedNumber
,a.QueueName
,a.Dept
,a.ffpNumber
from CRM_Loyalty_FFP a 
),

----------Join to Booking Pax Table on FFP number to get Booking info for active bookings

AllBookings_Pax as (
  Select
    a.*,
  --  b.contactid,
  --  b.ffpNumber,
    c.pnr,
    c.PAXNUM,
    c.FNAME,
    c.LNAME,
    d.pnrCreatedDate as BookingDate,
    c.FlightNumber
  from
    CombinedCRM_LoyaltyID_Phone_FFP a 
    left join ${raw_catalog}.${raw_sup_schema}.pnr_passengers c on a.ffpNumber = c.ffpNumber and c.dwh_current_flag = true and c.dwh_delete_flag = false
    left join ${raw_catalog}.${raw_sup_schema}.pnr_header d on c.pnr = d.pnr and d.dwh_current_flag = true and d.dwh_delete_flag = false
    and d.pnrCreatedDate between a.CallStart     and a.CallEndTime
),

----------Join to Booking Pax Table on FFP number to get Booking info for cancelled bookings
AllBookings_CNLPax as (
  Select
    a.*,
  --  b.contactid,
  --  b.ffpNumber,
    c.pnr,
    c.PAXNUM,
    c.FNAME,
    c.LNAME,
    d.pnrCreatedDate as BookingDate,
    c.FlightNumber
  from
    CombinedCRM_LoyaltyID_Phone_FFP a 
    left join ${raw_catalog}.${raw_sup_schema}.pnr_cancelled_passengers c on a.ffpNumber = c.ffpNumber and c.dwh_current_flag = true and c.dwh_delete_flag = false
    left join ${raw_catalog}.${raw_sup_schema}.pnr_header d on c.pnr = d.pnr and d.dwh_current_flag = true and d.dwh_delete_flag = false
    and d.pnrCreatedDate between a.CallStart
    and a.CallEndTime
),

--------Combine Active and Cancelled Pax--------
AllBookings as (
  Select
    *
  from
    AllBookings_Pax
  UNION
  Select
    *
  from
    AllBookings_CNLPax
),

---------Join Booking  to Call Call Table to return any results where booking was made at the time of the call
BookingsMatchID as (
  Select
    a.*,
    b.pnr,
    b.BookingDate,
    --b.contactid,
    --b.ffpNumber,
    b.FlightNumber,case
      when Upper(c.Status) = 'K' then 'CANCELLED'
      when (
        c.Status = ''
        and c.ReceivedAmt > 0
      )
      or Upper(c.ConfirmFlag) = 'Y' then 'CONFIRMED'
      when c.Status = ''
      and c.ReceivedAmt = 0 then 'OPTION'
      else 'Other'
    end as BookingStatus --INTO #BookingsMatchID
  from
    CombinedCRM_LoyaltyID_Phone_FFP a
    join AllBookings b on a.ffpNumber = b.ffpNumber
    and b.BookingDate between a.CallStart
    and a.CallEndTime
    and a.CallId = b.CallId
    left join  ${raw_catalog}.${raw_sup_schema}.pnr_sales c on b.pnr = c.pnr
)
*/
---------Join Booking  to Call Call Table to return any results where booking was made at the time of the call
,

final_final 
as (
Select
a.interactionId, a.department, a.skillname,'' as CampaignID,
a.Min_CallDateTime as Min_CallStart,
to_utc_timestamp(a.Min_CallDateTime, 'PST') as Min_CallStartUTC,
a.Metric,
  a.CallStart,
  to_utc_timestamp(a.CallStart, 'PST') as CallStartUTC,
  a.CallerNumber as Calling_Phone_Number,case
    when a.Dept = 'Res-Sales' then 1
    else 0
  end as Sales_Call,case
    when a.Dept = 'Res-Service' then 1
    else 0
  end as Service_Call,
  a.DialedNumber as TFN,
  b.pnr as BookingNumber,case
    when rb.pnr is not null then 'Rebook'
    when b.pnr is not null then 'New'
    else 'Non-Booking'
  end as Order_Type,
  b.NetFareRevenue_USD,
  Itinerary,
  Row_number() over (
        partition by a.interactionId
        order by
          a.interactionId,
          b.pnr,
          case
            when b.BookingStatus in ('CONFIRMED', 'FINAL') then 'A'
            when b.BookingStatus in ('OPTION') then 'B'
            when b.BookingStatus in ('CANCELLED') then 'C'
            else 'D'
          End
      ) as BookingOrder
from
AllCallBase a
  left join bookingsbase b on a.agentId = b.agentid_booking and b.BookingDate between a.CallStart and a.CallEndTime and b.rn=1
  left join (  -- Rebookings logic
    Select
      distinct pnr ----,CancelledPnr, CancelReason
    from
      (
        Select
          a.*,
          c.pnr as CancelledPnr,
          c.FNAME + ' ' + c.LNAME as CnaclledPaxName,
          s.CancelDate,
          sx.REASON as CancelReason
        from
          bookingsbase2 a
          --left join sup.loyalty_members b on a.contactid = b.contactid and b.dwh_current_flag = true and b.dwh_delete_flag = false
          left join ${raw_catalog}.${raw_sup_schema}.pnr_cancelled_passengers c on a.ffpNumber = c.ffpNumber and c.dwh_current_flag = true and c.dwh_delete_flag = false
          left join ${raw_catalog}.${raw_sup_schema}.pnr_sales s on c.pnr = s.pnr and s.dwh_current_flag = true and s.dwh_delete_flag = false
          left join ${raw_catalog}.${raw_sup_schema}.pnr_cancellations sx on c.pnr = sx.pnr and sx.dwh_current_flag = true and sx.dwh_delete_flag = false
        where
          a.pnr > c.pnr
          and cast(a.BookingDate as Date) = cast(s.CancelDate as Date)
      ) a
  ) rb on b.pnr = rb.pnr
)

,
final_data as 
(
    select interactionId, department,skillname, CampaignID,
    Min_CallStart,
    Min_CallStartUTC,
    Metric,
    CallStart,
    CallStartUTC,
    Calling_Phone_Number,
    Sales_Call,
    Service_Call,
    TFN,
    BookingNumber,
    Order_Type,
    NetFareRevenue_USD,
    Itinerary
    from final_final where bookingorder=1
)
,
FF_DD as 
(
    select *
    from final_data
    where cast(Min_CallStart as date)>='2021-12-22'
)
select * from FF_DD


-- COMMAND ----------

-- DBTITLE 1,AgentPresenceByDay
CREATE LIVE TABLE AgentPresenceByDay
AS
WITH AgentPresenceSummary
AS
(
SELECT 
  startTime, 
  agentId, 
  coalesce(COALESCE(endTime, LEAD(endTime) OVER (PARTITION BY agentId ORDER BY startTime ASC)), current_timestamp()) as endTime,
  FROM_UNIXTIME((UNIX_TIMESTAMP(startTime) div 1800) * 1800) AS startDate,
  coalesce(coalesce(FROM_UNIXTIME((UNIX_TIMESTAMP(endTime) div 1800) * 1800), FROM_UNIXTIME((UNIX_TIMESTAMP(LEAD(endTime) OVER (PARTITION BY agentId ORDER BY startTime ASC)) div 1800) * 1800)), FROM_UNIXTIME((UNIX_TIMESTAMP(current_timestamp()) div 1800) * 1800)) AS endDate,
  systemPresence, 
  organizationPresenceId
FROM ${gold_catalog}.${gold_schema}.AgentPrimaryPresence upp
), 
langLables AS 
(
  select 
  from_json(languageLabels, 'struct<en:string,en_US:string>').en_US as presence,id as presenceId
  from ${raw_catalog}.${raw_main_schema}.agent_presence_statuses
)

SELECT
  ursh.agentId, 
  ursh.startTime, 
  COALESCE(ursh.endTime, current_timestamp()) AS endTime,
  TRY_CAST(ursh.startDate AS TIMESTAMP) AS startDate, 
  TRY_CAST(ursh.endDate AS TIMESTAMP) AS endDate, 
  if(unix_timestamp(ursh.endTime) - unix_timestamp(startTime) < 0, 0, unix_timestamp(ursh.endTime) - unix_timestamp(startTime)) AS `duration_s`, 
  if(datediff(try_cast(endDate as date), try_cast(startDate as date)) < 0, 0, datediff(try_cast(endDate as date), try_cast(startDate as date))) AS `duration_d`,
  dsp.id AS primaryId, 
  dsp.systemPresence AS primary,
  ursh.organizationPresenceId AS secondaryId, 
  ll1.presence AS secondary
FROM AgentPresenceSummary AS ursh
JOIN ${raw_catalog}.${raw_main_schema}.agent_presence_statuses AS dsp ON ursh.organizationPresenceId = dsp.id
LEFT OUTER JOIN langLables AS ll1 on ll1.presenceId = ursh.organizationPresenceId

WHERE 1=1 
AND dsp.dwh_current_flag = 1
AND dsp.dwh_delete_flag = 0
GROUP BY 
 ursh.agentId
   ,ursh.startDate
   , ursh.endDate
	,ursh.startTime
	,ursh.endTime
	,ursh.organizationPresenceId
	,dsp.id
	,dsp.systemPresence
    ,ll1.presence

-- COMMAND ----------

-- DBTITLE 1,CampaignCallsAPI
CREATE LIVE TABLE CampaignCallsAPI
AS 
SELECT 
DATE_FORMAT(Min_CallStart,'yyyy-MM-dd hh:mm:ss a') AS call_start_time, 
CONCAT('+',Calling_Phone_Number) as calling_phone_number, 
Sales_Call AS sales_call, 
Service_Call AS service_call,
TFN,
CASE WHEN BookingNumber IS NULL THEN '' ELSE BookingNumber END AS booking_number,
Order_Type AS order_type,
CASE WHEN Itinerary IS NULL THEN '' ELSE Itinerary END  AS flight_number,
CASE WHEN BookingNumber > 0 THEN 1 ELSE 0 END AS booking,
COALESCE(G.GrossFareAmt,0.0) AS revenue,
CASE WHEN S.StatusText = 'OPTION' then 'CourtesyHold' WHEN S.StatusText = 'FINAL' THEN 'FullPayment' WHEN S.StatusText = 'CONFIRMED' THEN 'Deposit' WHEN S.StatusText = 'CANCELLED' THEN 'Cancelled' ELSE 'NA' END as booking_status,
I.interactionId as interactionId,
I.department AS department,
I.skillname AS skillname,
I.CampaignID as CampaignID,
I.NetFareRevenue_USD as NetFareRevenue_USD
FROM live.CampaignCalls I
--from reporting.CampaignCalls I
LEFT JOIN ${reporting_catalog}.${reporting_schema}.PnrRevenueGrouped G ON I.BookingNumber = G.pnr
LEFT JOIN ${reporting_catalog}.${reporting_schema}.PnrBookingStatus S ON I.BookingNumber = S.pnr
WHERE 
1=1
--AND cast(I.Min_CallStart as date) BETWEEN date_add(cast(current_date() AS DATE),-30) AND cast(current_date() AS DATE)
AND (sales_call <> 0 OR service_call <> 0  OR BookingNumber IS NOT NULL)  

-- COMMAND ----------

-- DBTITLE 1,PassengerCallSummary
CREATE LIVE TABLE PassengerCallSummary  PARTITIONED BY(partition_date)
AS 
With SplitBusinessUnitClean as (
Select 
    interactionId, 
    callLegId, 
    partyId, 
    Count(distinct(BusinessUnit)) as BusinessUnitCt
from live.PassengerCalls
where BusinessUnit in ('US Inbound Queues', 'Home')
group by interactionId, callLegId, partyId
having Count(distinct(BusinessUnit))  >1),


PassengerCallSummary AS 
(
Select 
    a.DirectionCode
    ,a.interactionId
    ,a.callLegId
    ,a.partyId
    ,a.AgentName
    ,a.PurposeCode
    ,a.agentId
    ,a.staffNumber
    ,a.CallerNumber
    ,a.DialedNumber
    ,a.QueueName
    ,case when b.interactionId is null then BusinessUnit else 'US Inbound Queues' end as BusinessUnit
    ,a.AgentBusinessUnit
    ,a.routingRing 
    ,a.agentBullseyeRing
    ,a.SupervisorName
    ,a.hireDate
    ,a.department
    ,a.title
    ,a.mediaType
    ,a.SkillName
    ,Min(EventDateTime_LAX) as StartDateTime
    ,sum(case when DirectionCode=1 and Metric ='tAnswered' then 1 else null end) as AnsweredInbound
    ,Max(case when DirectionCode=1 and Metric ='tAnswered' then value  else null end) as AnsweredValue
    ,Max(case when Metric ='tAbandon' then value else null end) as AbandonedValue
    ,Max(case when Metric ='nOverSla' then value else 0 end) as Answered_OverSLA
    ,Max(case when Metric ='tTalkComplete' then value else null end) as TotalTalkTimeValue
    ,Max(case when Metric ='tHandle' then value else null end) as HandledValue
    ,Max(case when Metric ='tAcw' then value else null end) as ACW_Time
    ,Max(case when Metric ='tHeldComplete' then value else null end) as HeldValue
    ,sum(case when Metric ='tHeldComplete' and value <> 0 then 1 else null end) as AnsweredHeld
    ,Max(case when Metric ='tHeldComplete' and value <> 0 then value else null end) as HeldValueTrue
    ,sum(case when Metric ='nTransferred' and value <> 0 then 1 else null end) as Transferred
    ,sum(case when DirectionCode=2 and Metric ='tAnswered' then 1 else null end) as CallBack_Count
    ,Max(case when DirectionCode=2 and Metric ='tAnswered' then value  else null end) as CallBack_AnsValue
    ,sum(case when DirectionCode=1 and Metric ='tShortAbandon' then 1 else null end) as ShortAbandon_Inbound
    ,Max(case when DirectionCode=1 and Metric ='tShortAbandon' then value  else null end) as ShortAbandon_Value

FROM live.PassengerCalls a
left join 	SplitBusinessUnitClean b on a.interactionId = b.interactionId and a.callLegId = b.callLegId and a.partyId = b.partyId						
	
where 
PurposeCode  in ('user', 'agent')  ---- Updated 08/29/2023
or (PurposeCode  in ('acd') and Metric in ( 'tAbandon', 'tShortAbandon'))---- Updated 08/29/2023


--and  a.interactionId = '21ad8ccf-2250-4d40-bb78-82c5f436e963' -- and a.callLegId ='6e21691d-aea3-4b89-a9ea-3b39c3253f10' and a.partyId ='829a1e29-c725-4586-90a3-1308b7f86cae'
Group by 
    a.DirectionCode
    ,a.interactionId
    ,a.callLegId
    ,a.partyId
    ,a.AgentName
    ,a.PurposeCode
    ,a.agentId
    ,a.staffNumber
    ,a.CallerNumber
    ,a.DialedNumber
    ,a.QueueName
    ,case when b.interactionId is null then BusinessUnit else 'US Inbound Queues' end 
    ,a.AgentBusinessUnit
    ,a.routingRing 
    ,a.agentBullseyeRing
    ,a.SupervisorName
    ,a.hireDate
    ,a.department
    ,a.title
    ,a.mediaType
    ,a.SkillName

--having Max(case when Metric ='tHandle' then value else null end) >0 or Max(case when Metric ='tAbandon' then value else null end) >0 or 
--Max(case when Metric ='tAnswered' then value else null end)>0 or Max(case when Metric ='tHeldComplete' then value else null end)>0 or 
--Max(case when Metric ='tTalkComplete' then value else null end)>0 or Max(case when Metric ='tAcw' then value else null end)>0
),

--Select * from PassengerCallSummary  where interactionId = '7844616e-1f8b-48bf-aea7-b0124750908a' 
--and callLegId = 'eb13e109-394a-452a-9d0d-c096a4511abd' and partyId = '7cb933c6-91d9-464a-8b51-c3046df477fa'




FirstRecord AS
(
Select interactionId, callLegId, partyId
    ,min(StartDateTime) as MinStartDateTime
    ,Max(coalesce(AnsweredInbound,0)) as AnsweredInbound 
    ,Max(coalesce(AnsweredValue,0)) as AnsweredValue 
    ,Max(coalesce(AbandonedValue,0)) as AbandonedValue 
    ,Max(coalesce(Answered_OverSLA,0)) as Answered_OverSLA 
    ,Max(coalesce(TotalTalkTimeValue,0)) as TotalTalkTimeValue 
    ,Max(coalesce(HandledValue,0)) as HandledValue 
    ,Max(coalesce(ACW_Time,0)) as ACW_Time 
    ,Max(coalesce(HeldValue,0)) as HeldValue 
    ,Max(coalesce(AnsweredHeld,0)) as AnsweredHeld 
    ,Max(coalesce(HeldValueTrue,0)) as HeldValueTrue 
    ,Max(coalesce(Transferred,0)) as Transferred 
    ,Max(coalesce(CallBack_Count,0)) as CallBack_Count 
    ,Max(coalesce(CallBack_AnsValue,0)) as CallBack_AnsValue 
    ,Max(coalesce(ShortAbandon_Inbound,0)) as ShortAbandon_Inbound 
    ,Max(coalesce(ShortAbandon_Value,0)) as ShortAbandon_Value 

from PassengerCallSummary
group by 
    interactionId
    ,callLegId
    ,partyId

)
--,

--Dup as (
Select 
    a.interactionId
    ,a.callLegId
    ,a.partyId
    ,a.MinStartDateTime as StartDateTime
    ,b.DirectionCode
    ,b.CallerNumber
    ,b.DialedNumber
    ,b.QueueName
    ,b.AgentName
    ,b.hireDate
    ,b.department
    ,b.title
    ,b.PurposeCode
    ,b.agentId
    ,b.staffNumber
    ,b.BusinessUnit
    ,b.AgentBusinessUnit
    ,b.SupervisorName
    ,cast(b.routingRing as int) as routingRing
    ,cast(b.agentBullseyeRing as int) as agentBullseyeRing
    ,b.mediaType
    ,cast(a.AnsweredInbound as int) as AnsweredInbound
    ,a.AnsweredValue 
    ,a.AbandonedValue 
    ,cast(a.Answered_OverSLA as int) as Answered_OverSLA
    ,a.TotalTalkTimeValue 
    ,a.HandledValue 
    ,a.ACW_Time 
    ,a.HeldValue 
    ,cast(a.AnsweredHeld as int) as AnsweredHeld
    ,a.HeldValueTrue 
    ,cast(a.Transferred as int) as Transferred
    ,cast(a.CallBack_Count as int) as CallBack_Count
    ,a.CallBack_AnsValue 
    ,cast(a.ShortAbandon_Inbound as int) as ShortAbandon_Inbound -- added 03-27-2023
    ,cast(a.ShortAbandon_Value as int) as ShortAbandon_Value   -- added 03-27-2023
    
	,Case
	when DirectionCode = '2' and a.AnsweredValue <> 0 and lower(mediaType) = 'callback' then 'Callback'
	when DirectionCode = '2' and Len(DialedNumber) >6 and lower(mediaType) = 'voice' and (lower(DialedNumber) not rlike '.*[A-Za-z].*' ) then 'Outbound_Passenger'
	when DirectionCode = '2' and DialedNumber rlike '.*\\*86' and lower(mediaType) = 'voice' then 'VM'
	when DirectionCode = '2' and lower(mediaType) = 'voice' and (Len(DialedNumber) <= 6 or lower(DialedNumber) rlike '.*[A-Za-z].*' or DialedNumber is null) then 'Outbound_Internal'
	when (DirectionCode = '1' and lower(PurposeCode) = 'user' and lower(mediaType) = 'voice') or (DirectionCode=1 and a.AnsweredValue = 0 and a.HandledValue >0 and lower(mediaType) = 'voice') then 'Direct_Ext'
	when DirectionCode = '1' and lower(PurposeCode) = 'agent' and a.AnsweredValue <> 0 and lower(mediaType) = 'voice' then 'Inbound_Queue'
	when DirectionCode = '1' and lower(PurposeCode) = 'agent' and a.AnsweredValue <> 0 and lower(mediaType) = 'message' then 'SMS_Inbound'
	when DirectionCode = '2' and lower(PurposeCode) = 'agent' and lower(mediaType) = 'message' then 'SMS_Outbound'
	When(lower(a.AbandonedValue) <> 0) then 'Abandoned'
	else 'Other'
	end as CallType --Updated 05-31-2023


    ,b.SkillName -- added 03/29/2023
    ,cast(a.MinStartDateTime as date) as partition_date

from FirstRecord a 
JOIN PassengerCallSummary b on a.interactionId = b.interactionId and a.callLegId = b.callLegId and a.partyId = b.partyId and a.MinStartDateTime = b.StartDateTime


/*
) select Count(*) as CT, interactionId, callLegId, partyId from Dup
group by interactionId, callLegId, partyId
having count(*) >1
*/

-- COMMAND ----------

-- DBTITLE 1,ResAgentsMonthly
CREATE LIVE TABLE ResAgentsMonthly
AS 

-- 1. Generate a list of months (bounded for performance)
WITH months AS (
  SELECT DISTINCT date_trunc('month', Date) AS month_start
  FROM ${reporting_catalog}.${reporting_schema}.dim_date
  WHERE date_trunc('month', Date) >= '2014-01-01'
    AND date_trunc('month', Date) <= date_trunc('month', current_date())
),

-- 2. Calculate key dates and statuses for each month
month_edges AS (
  SELECT
    m.month_start,
    cast((m.month_start + INTERVAL 8 DAYS) as timestamp)            AS som_ts,
    cast(add_months(m.month_start, 1) as timestamp)                 AS eom_ts,
    CASE
      WHEN m.month_start < date_trunc('month', current_date())      THEN 'HISTORICAL'
      WHEN current_date() < (m.month_start + INTERVAL 8 DAYS)        THEN 'CURRENT_PRE_9'
      WHEN current_date() = last_day(current_date())                 THEN 'CURRENT_LAST_DAY'
      ELSE 'CURRENT_MID_MONTH'
    END                                                             AS month_status
  FROM months m
),

-- 3. Base SOM snapshot (raw data)
som_raw AS (
  SELECT e.month_start AS month, u.*
  FROM ${raw_catalog}.${raw_main_schema}.agents u
  JOIN month_edges e
    ON u.dwh_valid_from <= e.som_ts AND u.dwh_valid_to > e.som_ts AND e.month_start >= date_trunc('month', u.hireDate)
  WHERE u.state = 'active'
),

-- 4. Base EOM snapshot (raw data)
eom_raw AS (
  SELECT e.month_start AS month, u.*
  FROM ${raw_catalog}.${raw_main_schema}.agents u
  JOIN month_edges e
    ON u.dwh_valid_from <= e.eom_ts AND u.dwh_valid_to > e.eom_ts AND e.month_start >= date_trunc('month', u.hireDate)
  WHERE u.state = 'active'
),

-- 5. CTEs with explicit column projection to ensure safe UNION ALL operations
som_proj AS (
  SELECT
    s.month, s.id, s.staffNumber, s.name, s.email, s.hireDate, s.businessUnitName, s.department, 
    s.title, s.state, s.supervisorId,
    s.dwh_delete_flag, s.dwh_current_flag, s.dwh_valid_from, s.dwh_valid_to, 
    s.locationId1, s.locationId2
  FROM som_raw s
),
eom_proj AS (
  SELECT
    e.month, e.id, e.staffNumber, e.name, e.email, e.hireDate, e.businessUnitName, e.department, 
    e.title, e.state, e.supervisorId,
    e.dwh_delete_flag, e.dwh_current_flag, e.dwh_valid_from, e.dwh_valid_to, 
    e.locationId1, e.locationId2
  FROM eom_raw e
),
prev_eom_to_this AS (
  SELECT
    add_months(r.month, 1) AS month,
    r.id, r.staffNumber, r.name, r.email, r.hireDate, r.businessUnitName, r.department, 
    r.title, r.state, r.supervisorId, 
    r.dwh_delete_flag, r.dwh_current_flag, r.dwh_valid_from, r.dwh_valid_to,
    r.locationId1, r.locationId2
  FROM eom_raw r
),

-- 6. Assemble final SOM data using the projected CTEs
final_som AS (
  SELECT sp.* FROM som_proj sp JOIN month_edges me ON me.month_start = sp.month AND me.month_status <> 'CURRENT_PRE_9'
  UNION ALL
  SELECT p.* FROM prev_eom_to_this p JOIN month_edges me ON me.month_start = p.month AND me.month_status = 'CURRENT_PRE_9'
),

-- 7. Assemble final EOM data using the projected CTEs
final_eom AS (
  SELECT ep.* FROM eom_proj ep JOIN month_edges me ON me.month_start = ep.month AND (me.month_status IN ('HISTORICAL', 'CURRENT_LAST_DAY'))
  UNION ALL
  SELECT p.* FROM prev_eom_to_this p JOIN month_edges me ON me.month_start = p.month AND me.month_status = 'CURRENT_PRE_9'
  UNION ALL
  SELECT sp.* FROM final_som sp JOIN month_edges me ON me.month_start = sp.month AND me.month_status = 'CURRENT_MID_MONTH'
),

-- 8. Final join to produce the required output
-- FIX: COALESCE on partition to handle NULL EOM matches (recovers 29 users)
user_base_pre AS (
  SELECT
    s.month                 AS month,
    s.id                    AS id,
    s.dwh_valid_from        AS dwh_valid_from,
    s.dwh_valid_to          AS dwh_valid_to,
    s.staffNumber            AS staffNumber,
    s.name                  AS name,
    s.email                 AS email,
    s.hireDate              AS hireDate,
    s.businessUnitName          AS businessUnitName,
    s.department            AS Dept_StartofMonth,
    e.department            AS Dept_EodMonth,
    s.title                 AS Title_StartofMonth,
    e.title                 AS Title_EodofMonth,
    s1.id                   AS supervisorId_StartOfMonth,
    s1.name                 AS supervisor_StartOfMonth,
    s2.id                   AS supervisorId_EndOfMonth,
    s2.name                 AS supervisor_EndOfMonth,
    COALESCE(s.locationId1, s.locationId2) AS locationId,
    row_number() over(partition by COALESCE(e.month, s.month), COALESCE(e.email, s.email) order by e.dwh_valid_from desc) as rn
  FROM final_som s
  LEFT JOIN final_eom e
    ON s.id = e.id AND s.month = e.month
  LEFT JOIN (SELECT DISTINCT name, id FROM ${raw_catalog}.${raw_main_schema}.agents WHERE dwh_current_flag = 'true') s1
    ON s.supervisorId = s1.id
  LEFT JOIN (SELECT DISTINCT name, id FROM ${raw_catalog}.${raw_main_schema}.agents WHERE dwh_current_flag = 'true') s2
    ON e.supervisorId = s2.id
),

AllUserid as (
  SELECT * FROM ${reporting_catalog}.${reporting_schema}.contact_res_agent_link 
),

ContactAgentBase as (
  SELECT a.month,
         a.id as agentId,
         a.staffNumber,
         a.name,
         a.email,
         a.hireDate,
         a.businessUnitName,
         a.Dept_StartofMonth,
         a.Dept_EodMonth,
         a.Title_StartofMonth,
         a.Title_EodofMonth,
         b.Res_AgentID1,
         b.Res_AgentID2,
         b.Res_AgentID3,
         b.Res_AgentID4,
         b.Res_AgentID5,
         b.Res_AgentID6,
         b.Res_AgentID7,
         b.Res_AgentID8,
         a.supervisor_EndOfMonth as supervisor_EndOfMonth,
         a.supervisorId_EndOfMonth,
         a.supervisor_StartOfMonth as supervisor_StartOfMonth,
         a.supervisorId_StartOfMonth,
         a.locationId
  FROM user_base_pre a
  LEFT JOIN AllUserid b ON a.id = b.Contact_AgentID
  WHERE a.rn = 1 
),

base_hr as (
  SELECT Lpad(staffNumber, 11, '0') as staffNumber,
         state,
         lower(trim(workemail)) as workemail,
         name,
         CASE WHEN dwh_valid_from = '1900-01-01T00:00:00.000' THEN dwh_load_date ELSE dwh_valid_from END as clean_valid_from,
         dwh_valid_to,
         leaveDate
  FROM ${raw_catalog}.${raw_generic_schema}.hr_staff
  WHERE dwh_delete_flag = 'false'
    AND state IS NOT NULL
),

nextstatedata as (
  SELECT *,
         lead(clean_valid_from) over(partition by staffNumber, workemail order by clean_valid_from) as next_valid_from
  FROM base_hr
),

final_hr as (
  SELECT staffNumber,
         state as workerscompcodestate,
         name,
         leaveDate,
         workemail,
         clean_valid_from as dwh_valid_from,
         CASE WHEN next_valid_From IS NOT NULL AND dwh_valid_to < next_valid_from THEN next_valid_From
              ELSE dwh_valid_to END as dwh_valid_to
  FROM nextstatedata
),

ResMonthlyUsers_pre as (
  SELECT a.*,
         b.workersCompCodeState,
         b.leaveDate,
         b.dwh_valid_from,
         b.dwh_valid_to,
         ROW_NUMBER() over (
           partition by Lpad(a.staffNumber, 11, '0'), a.month, lower(Substring(a.email, 0, charindex('@', a.email)-1)) 
           order by Lpad(a.staffNumber, 11, '0'), a.month, b.dwh_valid_to desc, b.dwh_valid_from, 
                    CASE WHEN a.email like '%example-air.com%' THEN 0 ELSE 1 END desc
         ) as dateflag
  FROM ContactAgentBase a
  LEFT JOIN final_hr b 
    ON (Lpad(a.staffNumber, 11, '0') = lpad(b.staffNumber, 11, '0')
        OR lower(Substring(a.email, 0, charindex('@', a.email)-1)) = Lower(Substring(b.workEmail, 0, charindex('@', b.workEmail)-1))
        OR trim(lower(a.name)) = trim(lower(b.name)))
    AND a.month >= b.dwh_valid_from AND a.month < b.dwh_valid_to
),

ResMonthlyUsers_base as (
  SELECT month,
         name,
         agentId,
         workersCompCodeState,
         max(leaveDate) as leaveDate,
         Lpad(staffNumber, 11, '0') as staffNumber
  FROM ResMonthlyUsers_pre
  WHERE dateflag = 1
  GROUP BY month, name, agentId, workersCompCodeState, Lpad(staffNumber, 11, '0')
),

ResMonthlyUsers_final as (
  SELECT *
  FROM ResMonthlyUsers_base
  WHERE workersCompCodeState IS NOT NULL
),

MaxEmployeeId_A as (
  SELECT ID, Max(dwh_valid_from) as Max_dwh_valid_From
  FROM ${raw_catalog}.${raw_main_schema}.agents
  WHERE state = 'active' AND staffNumber IS NOT NULL
  -- Note: dwh_delete_flag filter intentionally omitted to capture terminated agents' final staffNumber
  GROUP BY ID
),

MaxEmployeeId_B as (
  SELECT a.*, 
         b.staffNumber,
         row_number() over(partition by a.id, b.staffNumber order by a.Max_dwh_valid_From desc) as mrn
  FROM MaxEmployeeId_A a
  JOIN ${raw_catalog}.${raw_main_schema}.agents b ON a.id = b.id AND a.Max_dwh_valid_From = b.dwh_valid_from
),

hr_term as (
  SELECT * 
  FROM ${raw_catalog}.${raw_generic_schema}.hr_staff 
  WHERE dwh_delete_flag = 'false'
    AND dwh_current_flag = 'true'
),

-- Prioritized match for hr_term: staffNumber (1) > email prefix (2) > name (3)
-- This replaces the OR-based join to prevent false-positive name matches and improves performance
hr_match_all AS (
  -- Priority 1: Match by staffNumber
  SELECT a.agentId, a.month, d.leaveDate, d.staffNumber as d_employeeId, d.workEmail as d_workEmail, d.name as d_name, 1 as match_rank
  FROM ContactAgentBase a
  JOIN ResMonthlyUsers_final b ON a.agentId = b.agentId AND a.Month = b.month
  JOIN hr_term d ON Lpad(b.staffNumber, 11, '0') = lpad(d.staffNumber, 11, '0')
  UNION ALL
  -- Priority 2: Match by email prefix
  SELECT a.agentId, a.month, d.leaveDate, d.staffNumber, d.workEmail, d.name, 2
  FROM ContactAgentBase a
  JOIN hr_term d ON lower(Substring(a.email, 0, charindex('@', a.email))) = Lower(Substring(d.workEmail, 0, charindex('@', d.workEmail)))
  UNION ALL
  -- Priority 3: Match by name (last resort)
  SELECT a.agentId, a.month, d.leaveDate, d.staffNumber, d.workEmail, d.name, 3
  FROM ContactAgentBase a
  JOIN hr_term d ON lower(trim(a.name)) = trim(lower(d.name))
),

hr_best_match AS (
  SELECT agentId, month, leaveDate,
         ROW_NUMBER() OVER (PARTITION BY agentId, month ORDER BY match_rank) as mr
  FROM hr_match_all
),

final_dataset as (
  SELECT a.month,
         a.name,
         a.agentId,
         a.staffNumber as staffNumber_CC,
         c.staffNumber as staffNumber,
         a.hireDate,
         a.businessUnitName,
         a.Dept_StartofMonth,
         CASE WHEN a.Dept_EodMonth IS NULL THEN a.Dept_StartofMonth ELSE a.Dept_EodMonth END as Dept_EndofMonth,
         a.Title_StartofMonth,
         CASE WHEN a.Title_EodofMonth IS NULL THEN a.Title_StartofMonth ELSE a.Title_EodofMonth END as Title_EndofMonth,
         a.email,
         d.leaveDate as leftDate,
         CASE WHEN a.month = date_trunc('mm', d.leaveDate) THEN 'Termed' ELSE 'active' END as Status,
         a.Res_AgentID1,
         a.Res_AgentID2,
         a.Res_AgentID3,
         a.Res_AgentID4,
         a.Res_AgentID5,
         a.Res_AgentID6,
         a.Res_AgentID7,
         a.Res_AgentID8,
         a.supervisor_StartOfMonth as supervisor_StartOfMonth,
         a.supervisorId_StartOfMonth as supervisorId_StartOfMonth,
         CASE WHEN a.supervisor_EndOfMonth IS NULL THEN a.supervisor_StartOfMonth ELSE a.supervisor_EndOfMonth END as supervisor_EndOfMonth,
         CASE WHEN a.supervisorId_EndOfMonth IS NULL THEN a.supervisorId_StartOfMonth ELSE a.supervisorId_EndOfMonth END as supervisorId_EndOfMonth,
         a.locationId,
         b.workersCompCodeState,
         -- FIX: COALESCE prevents NULL staffNumber collapse (820 people with NULL would all merge per month)
         row_number() over(partition by a.month, COALESCE(c.staffNumber, a.agentId) order by a.hireDate desc, CASE WHEN a.email like '%example-air.com%' THEN 0 ELSE 1 END desc) as frk
  FROM ContactAgentBase a
  LEFT JOIN ResMonthlyUsers_final b ON a.agentId = b.agentId AND a.Month = b.month
  LEFT JOIN MaxEmployeeId_B c ON a.agentId = c.id AND c.mrn = 1
  LEFT JOIN hr_best_match d ON a.agentId = d.agentId AND a.month = d.month AND d.mr = 1
  WHERE (d.leaveDate IS NULL OR a.month <= date_trunc('mm', d.leaveDate))
)

SELECT * FROM final_dataset 
WHERE frk = 1
