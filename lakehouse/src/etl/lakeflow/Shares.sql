-- Databricks notebook source
-- DBTITLE 1,VCabin_Classes
CREATE TEMPORARY VIEW VCabin_Classes
AS
SELECT 
	li.REF_CODE_ID
   ,li.REF_CODE_NAME
   ,li.ACTIVE
   ,li.REC_DELETED
FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') li
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_categories') sc
	ON li.REF_CATEGORY_ID = sc.REF_CATEGORY_ID
WHERE (sc.REF_CATEGORY_NAME = 'Cabin Classes')
AND li.dwh_current_flag = true
AND li.dwh_delete_flag = false
AND sc.dwh_current_flag = true
AND sc.dwh_delete_flag = false

-- COMMAND ----------

-- DBTITLE 1,VBoarding_Method
CREATE TEMPORARY VIEW VBoarding_Method
AS
SELECT 
	sys_ref_codes.SYS_REF_ID
   ,sys_ref_codes.SYS_REF_NAME
   ,sys_ref_codes.SYS_REF_DISPLAY_CODE
   ,sys_ref_codes.SYS_REF_ACTIVE
   ,sys_ref_codes.REC_DELETED
FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.sys_ref_categories') AS sys_ref_categories
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.sys_ref_codes') AS sys_ref_codes
	ON sys_ref_categories.SYS_REF_CATEGORY_ID = sys_ref_codes.SYS_REF_CATEGORY_ID
WHERE sys_ref_categories.SYS_REF_CATEGORY_NAME = 'Boarding Methods'
AND sys_ref_categories.dwh_current_flag = true
AND sys_ref_categories.dwh_delete_flag = false
AND sys_ref_codes.dwh_current_flag = true
AND sys_ref_codes.dwh_delete_flag = false

-- COMMAND ----------

-- DBTITLE 1,VMeal_Services
CREATE TEMPORARY VIEW VMeal_Services
AS
SELECT
	ref_codes.REF_CODE_ID
   ,ref_codes.REF_CODE_NAME
   ,ref_codes.ACTIVE
   ,ref_codes.REC_DELETED
FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') AS ref_codes
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_categories') AS ref_categories
	ON ref_codes.REF_CATEGORY_ID = ref_categories.REF_CATEGORY_ID
WHERE ref_categories.REF_CATEGORY_NAME = 'Meal Services'
AND ref_codes.dwh_current_flag = true
AND ref_codes.dwh_delete_flag = false
AND ref_categories.dwh_current_flag = true
AND ref_categories.dwh_delete_flag = false

-- COMMAND ----------

-- DBTITLE 1,VFlight_Legs
CREATE TEMPORARY VIEW VFlight_Legs
AS
SELECT
	flight_legs.FLIGHT_LEG_ID
   ,fleet_units.FLEET_UNIT_NAME AS AIRCRAFT_NAME
   ,fleet_units.FLEET_UNIT_CODE AS AIRCRAFT_REGISTRATION
   ,fleet_units.PSS_CODE
   ,fleet_units.WEB_BOOKING_CODE
   ,fleet_units.REVENUE_ACCOUNTING_CODE
   ,flight_legs.FLIGHT_ID
   ,flights.FLIGHT_NUMBER
   ,flight_legs.LEG_DATE
   ,flight_legs.DAY_OF_JOURNEY
   ,stations.STATION_IATA_CODE AS AIRPORT_IATA_CODE
   ,stations.STATION_ICAO_CODE AS AIRPORT_ICAO_CODE
   ,stations.STATION_NAME AS AIRPORT_NAME
   ,stations.STATION_PHONETIC_NAME AS AIRPORT_NAME_PHONETIC
   ,flight_legs.ETA_LOCAL
   ,flight_legs.ETD_LOCAL
   ,flight_legs.STA
   ,flight_legs.STD
   ,flight_legs.UTC_OFFSET_DIFF
   ,VBoarding_Method.SYS_REF_DISPLAY_CODE AS BOARDING_METHOD_CODE
   ,VBoarding_Method.SYS_REF_NAME AS BOARDING_METHOD_NAME
   ,terminal_gates.TERMINAL_GATE_NAME AS GATE_NAME
   ,Countries.COUNTRY_ISO_CODE
   ,Countries.COUNTRY_NAME
   ,Countries.COUNTRY_LOCAL_NAME
   ,flights.CHARTER_FLAG
   ,flight_legs.OPS_REMARKS
   ,flight_legs.LAST_UPDATED_TS
   ,stations.STATION_ALIAS_NAME
   ,stations.DEFAULT_TERMINAL_NAME
   ,VCabin_Classes.REF_CODE_NAME AS Cabin_Class_Name
   ,VMeal_Services.REF_CODE_NAME AS Meal_Service_Name
   ,stations.STATION_TIMEZONE
   ,journey_days.DELETED AS Flight_Day_Deleted_Status
   ,flight_legs.DELETED AS Leg_Deleted_Status
   ,flights.DELETED AS Flight_Deleted_Status
   ,stations.ACTIVE AS Airport_Active_Status
   ,flights.FLEET_UNIT_ID
   ,flights.DISTRIBUTE_TO_WEB
   ,flights.DISTRIBUTE_TO_INTRANET
   ,flights.DISTRIBUTE_TO_PSS
   ,flight_legs.AIRPORT_ID
   ,flight_legs.GUID
   ,flights.ROUTE_THEME
   ,stations.IS_EN_ROUTE_POINT
   ,flights.FLIGHT_TYPE
   ,flight_legs.TURNAROUND_STATION
   ,flight_legs.LEG_SEQUENCE
   ,flights.SCHEDULE_STATUS
   ,flights.ACTIVE

FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.fleet_units') AS fleet_units
LEFT OUTER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') AS flights
	ON fleet_units.FLEET_UNIT_ID = flights.FLEET_UNIT_ID
		AND flights.dwh_current_flag = true
		AND flights.dwh_delete_flag = false

RIGHT OUTER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flight_legs') AS flight_legs
	ON flights.FLIGHT_ID = flight_legs.FLIGHT_ID
		AND flight_legs.dwh_current_flag = true
		AND flight_legs.dwh_delete_flag = false  

LEFT OUTER JOIN VBoarding_Method
	ON VBoarding_Method.SYS_REF_ID = flight_legs.BOARDING_METHOD

INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.journey_days') AS journey_days
ON flight_legs.FLIGHT_ID = journey_days.FLIGHT_ID
		AND flight_legs.DAY_OF_JOURNEY = journey_days.DAY_OF_JOURNEY
		AND journey_days.dwh_current_flag = true
		AND journey_days.dwh_delete_flag = false

LEFT OUTER JOIN VMeal_Services
ON VMeal_Services.REF_CODE_ID = journey_days.MEAL_SERVICE_ID
   
LEFT OUTER JOIN VCabin_Classes
ON journey_days.CABIN_CLASS_ID = VCabin_Classes.REF_CODE_ID

LEFT OUTER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.terminal_gates') AS terminal_gates
	ON flight_legs.GATE_ID = terminal_gates.TERMINAL_GATE_ID
		AND terminal_gates.dwh_current_flag = true
		AND terminal_gates.dwh_delete_flag = false

LEFT OUTER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') AS stations
	ON flight_legs.AIRPORT_ID = stations.STATION_ID
		AND stations.dwh_current_flag = true
		AND stations.dwh_delete_flag = false

LEFT OUTER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.countries') AS countries
	ON Countries.COUNTRY_ID = stations.COUNTRY_ID
		AND Countries.dwh_current_flag = true
		AND Countries.dwh_delete_flag = false    

WHERE flight_legs.DELETED = false
AND journey_days.DELETED = false
AND flights.DELETED = false
AND flight_legs.NON_REVENUE_FLAG = false
AND fleet_units.dwh_current_flag = true
AND fleet_units.dwh_delete_flag = false

-- COMMAND ----------

-- DBTITLE 1,VWorld_Regions
CREATE TEMPORARY VIEW VWorld_Regions
AS
SELECT     
      ref_codes.REF_CODE_ID, 
      ref_codes.REF_CODE_NAME

FROM          IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') AS ref_codes 
INNER JOIN    IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_categories') AS ref_categories 
ON            ref_codes.REF_CATEGORY_ID = ref_categories.REF_CATEGORY_ID

WHERE     (ref_categories.REF_CATEGORY_NAME = 'World Regions')
		AND ref_codes.dwh_current_flag = true
		AND ref_codes.dwh_delete_flag = false
		AND ref_categories.dwh_current_flag = true
		AND ref_categories.dwh_delete_flag = false

ORDER BY ref_codes.REF_CODE_NAME

-- COMMAND ----------

-- DBTITLE 1,VStations
CREATE TEMPORARY VIEW VStations
AS
SELECT     
          stations.STATION_ID, 
          stations.STATION_NAME, 
          Countries.COUNTRY_NAME, 
          VWorld_Regions.REF_CODE_NAME AS WorldRegionName, 
          stations.STATION_ICAO_CODE, 
          stations.STATION_IATA_CODE, 
          stations.ACTIVE, 
          stations.STATION_ALIAS_NAME, 
          stations.STATION_ROUTING_NAME, 
          Countries.COUNTRY_ISO_CODE, 
          Countries.COUNTRY_ISO3_CODE

FROM        IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') AS stations 
LEFT OUTER JOIN  IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.countries') AS countries 
ON stations.COUNTRY_ID = Countries.COUNTRY_ID 
LEFT OUTER JOIN VWorld_Regions 
ON Countries.WORLD_REGION_ID = VWorld_Regions.REF_CODE_ID

WHERE
			stations.dwh_current_flag = true
		AND stations.dwh_delete_flag = false
		AND Countries.dwh_current_flag = true
		AND Countries.dwh_delete_flag = false

-- COMMAND ----------

-- DBTITLE 1,AncillaryOfferScoreRanking
-----
----- Ancillary offers
-----
CREATE OR REFRESH MATERIALIZED VIEW AncillaryOfferScoreRanking 
AS
WITH keytable AS (
    SELECT DISTINCT
        flights.FLIGHT_NUMBER AS FlightNo,
        flights.SCHED_DEPARTURE_DATE,
        CASE
            WHEN VStations.STATION_NAME LIKE '%In flight%' THEN 'In flight'
            WHEN flights.FLIGHT_NUMBER = 'SX0104' AND TPI.ANCILLARY_OFFER_CODE = 'NAPCTY01' AND VStations.STATION_NAME = 'Nashville' THEN 'Naples'
            WHEN flights.SCHED_DEPARTURE_DATE = '2016-10-02' AND ROUTE_MARKETING_NAME = 'East Coast Air Journey' AND TPI.ANCILLARY_OFFER_CODE LIKE '%YBGCTY01%' THEN 'Saguenay'
            ELSE VStations.STATION_NAME
        END AS Station,
        service_items.RFISC_CODE AS AncillaryCode,
        TPI.LEG_DATE AS ServiceDate,
        service_items.SERVICE_ITEM_NAME AS AncillaryName
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI
    LEFT JOIN VStations ON VStations.STATION_ID = TPI.STATION_ID
    AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM 
    ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID AND TPM.REC_DELETED = false AND WAITLISTED = false
    AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') AS flights ON flights.FLIGHT_ID = TPI.FLIGHT_ID
    AND flights.dwh_current_flag = true and flights.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') AS ancillary_products ON TPI.ANCILLARY_PRODUCT_ID = ancillary_products.ANCILLARY_PRODUCT_ID
    AND ancillary_products.dwh_current_flag = true and ancillary_products.dwh_delete_flag = FALSE  
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items ON service_items.SERVICE_ITEM_ID = ancillary_products.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE    
    WHERE ancillary_products.REC_DELETED = false
        AND TPI.OFFER_STATUS_ID = 0
        AND TPI.REC_DELETED = false
        AND flights.SCHED_DEPARTURE_DATE > DATEADD(YEAR, -1, GETDATE())
        AND TPI.ANCILLARY_CATEGORY_ID = 12
),

AncillaryBooking AS (
    SELECT
        flights.FLIGHT_NUMBER AS FlightNo,
        CASE
            WHEN VStations.STATION_NAME LIKE '%In flight%' THEN 'In flight'
            WHEN flights.FLIGHT_NUMBER = 'SX0104' AND TPI.ANCILLARY_OFFER_CODE = 'NAPCTY01' AND VStations.STATION_NAME = 'Nashville' THEN 'Naples'
            --WHEN flights.SCHED_DEPARTURE_DATE = '2016-10-02' AND ROUTE_MARKETING_NAME = 'East Coast Air Journey' AND TPI.ANCILLARY_OFFER_CODE LIKE '%YBGCTY01%' THEN 'Saguenay'
            ELSE VStations.STATION_NAME
        END AS Station,
        TPI.LEG_DATE AS ServiceDate,
        service_items.RFISC_CODE AS AncillaryCode,
        CASE
            WHEN TPI.ANCILLARY_OFFER_NAME LIKE '%bundled%' THEN 'Bnd'
            WHEN TPI.ANCILLARY_CATEGORY_ID NOT IN (12,13,14) THEN 'Oth'
            WHEN LI.REF_CODE_NAME LIKE '%Ancillary Misc Costs%' THEN 'Oth'
            WHEN SUM(TPM.EMD_AMOUNT) > 0 THEN 'Opt'
            WHEN SUM(TPM.EMD_AMOUNT) = 0 THEN 'Bnd'
            WHEN SUM(TPM.EMD_AMOUNT) IS NULL THEN 'Bnd'
        END AS AncillaryType,
        SUM(TPM.EMD_AMOUNT) AS NetAncillaryRevenueUSD,
        COUNT(passengers.PASSENGER_ID) AS BookedPax
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI
    LEFT JOIN VStations ON VStations.STATION_ID = TPI.STATION_ID
    AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID 
    AND TPM.REC_DELETED = false AND WAITLISTED = FALSE
    AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passengers') AS passengers ON passengers.PASSENGER_ID = TPM.PASSENGER_ID
    AND passengers.dwh_current_flag = true and passengers.dwh_delete_flag = FALSE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') AS flights ON flights.FLIGHT_ID = TPI.FLIGHT_ID
    AND flights.dwh_current_flag = true and flights.dwh_delete_flag = FALSE    
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') AS ancillary_products ON TPI.ANCILLARY_PRODUCT_ID = ancillary_products.ANCILLARY_PRODUCT_ID
    AND ancillary_products.dwh_current_flag = true and ancillary_products.dwh_delete_flag = FALSE       
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items ON service_items.SERVICE_ITEM_ID = ancillary_products.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE      
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_item_accounting') IMA ON IMA.SERVICE_ITEM_ID = ancillary_products.SERVICE_ITEM_ID
    AND IMA.dwh_current_flag = true and IMA.dwh_delete_flag = FALSE       
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') LI ON LI.REF_CODE_ID = IMA.GL_ACCOUNT_ID
    AND LI.dwh_current_flag = true and LI.dwh_delete_flag = FALSE       
    WHERE ancillary_products.REC_DELETED = false
        AND TPI.OFFER_STATUS_ID = 0
        AND TPI.REC_DELETED = false
        AND TPI.SALE_ACTIVE = true
        AND flights.SCHED_DEPARTURE_DATE > DATEADD(YEAR, -1, GETDATE())
    GROUP BY flights.FLIGHT_NUMBER, VStations.STATION_NAME, TPI.LEG_DATE, service_items.RFISC_CODE, TPI.ANCILLARY_OFFER_CODE, TPI.ANCILLARY_OFFER_NAME, LI.REF_CODE_NAME, TPI.ANCILLARY_CATEGORY_ID
),

suppliercosttable AS (
    SELECT
        flights.FLIGHT_NUMBER,
        stations.STATION_NAME,
        CAST(IDO.SERVICE_DATE AS DATE) AS ServiceDate,
        service_items.RFISC_CODE AS AncillaryCode,
        service_items.SERVICE_ITEM_NAME AS AncillaryName,
        suppliers.SUPPLIER_NAME AS Supplier,
        CAST(AVG(IDE.UNIT_COST) * IDO.FX_RATE_TO_USD AS decimal(10,2)) AS AvgSupplierUnitCost,
        SUM(QTY_BILLED) AS PaxBilled,
        SUM(QTY_FLOWN) AS PaxFlown,
        CAST(SUM(IDE.LINE_COST) * IDO.FX_RATE_TO_USD AS decimal(10,2)) AS TotalSupplierCost
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.supplier_invoice_lines') IDE
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.supplier_invoices') IDO ON IDO.SUPPLIER_INVOICE_ID = IDE.SUPPLIER_INVOICE_ID
    AND IDE.dwh_current_flag = true and IDE.dwh_delete_flag = FALSE 
    AND IDO.dwh_current_flag = true and IDO.dwh_delete_flag = FALSE     
    INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.suppliers') AS suppliers ON IDO.SUPPLIER_ID = suppliers.SUPPLIER_ID
    AND suppliers.dwh_current_flag = true and suppliers.dwh_delete_flag = FALSE    
    INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.currencies') AS currencies ON IDO.CURRENCY_ID = Currencies.CURRENCY_ID
    AND currencies.dwh_current_flag = true and currencies.dwh_delete_flag = FALSE     
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items ON IDE.SERVICE_ITEM_ID = service_items.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE       
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') AS flights ON flights.FLIGHT_ID = IDO.FLIGHT_ID
    AND flights.dwh_current_flag = true and flights.dwh_delete_flag = FALSE      
    INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') AS stations ON IDO.SERVICE_STATION_ID = stations.STATION_ID
    AND stations.dwh_current_flag = true and stations.dwh_delete_flag = FALSE     
    WHERE IDO.INVOICE_STATUS_ID IN (1,30)
        AND IDO.INVOICE_TYPE_ID = 6
        AND IDE.LINE_COST <> 0
        AND flights.SCHED_DEPARTURE_DATE > DATEADD(YEAR, -1, GETDATE())
    GROUP BY flights.FLIGHT_NUMBER, stations.STATION_NAME, IDO.SERVICE_DATE, service_items.RFISC_CODE, service_items.SERVICE_ITEM_NAME, suppliers.SUPPLIER_NAME, IDO.FX_RATE_TO_USD
),


filtertbl as (
SELECT
    keytable.FlightNo AS FLIGHT_NUMBER,
    keytable.Station AS STATION_NAME,
    keytable.ServiceDate,
    keytable.AncillaryCode,
    keytable.AncillaryName,
    suppliercosttable.Supplier,
    suppliercosttable.AvgSupplierUnitCost,
    suppliercosttable.PaxBilled,
    suppliercosttable.PaxFlown,
    suppliercosttable.TotalSupplierCost,
    AncillaryBooking.AncillaryType,
    AncillaryBooking.NetAncillaryRevenueUSD,
    AncillaryBooking.BookedPax

FROM keytable
LEFT JOIN AncillaryBooking ON keytable.FlightNo = AncillaryBooking.FlightNo
    AND keytable.Station = AncillaryBooking.Station
    AND keytable.ServiceDate = AncillaryBooking.ServiceDate
    AND keytable.AncillaryCode = AncillaryBooking.AncillaryCode
LEFT JOIN suppliercosttable ON keytable.FlightNo = suppliercosttable.FLIGHT_NUMBER
    AND keytable.Station = suppliercosttable.STATION_NAME
    AND keytable.ServiceDate = suppliercosttable.ServiceDate
    AND keytable.AncillaryCode = suppliercosttable.AncillaryCode
WHERE keytable.SCHED_DEPARTURE_DATE > DATEADD(YEAR, -1, GETDATE())
),

mrev as (
select 
AncillaryCode,
sum(COALESCE(NetAncillaryRevenueUSD,0)) as M_REV
from filtertbl
where Supplier is not null 
and ServiceDate < dateadd(MONTH , -1, getdate())
group by 
AncillaryCode
),

mcost as (
select 
	AncillaryCode, 
	sum(M_COST) as M_COST

from ( 
		 select 
		ServiceDate,
		AncillaryCode,
		FLIGHT_NUMBER,
     	max( COALESCE(TotalSupplierCost,0)) as M_COST
		from filtertbl
		where Supplier is not null 
		and ServiceDate < dateadd(MONTH , -1, getdate())
		group by 
		ServiceDate,
		AncillaryCode,
		FLIGHT_NUMBER
		) cost
GROUP BY AncillaryCode
),

margin as (
select 
distinct
ft.AncillaryCode,

---  The check here is to mimic below DAX logic
---
---  VAR MarginValue = IF(
---  [Margin %] = -1/0 || [Margin %] = 1/0, 0, VALUE([Margin %]))

CASE 
  WHEN mr.M_REV = 0 OR mr.M_REV IS NULL THEN 0
  ELSE (mr.M_REV - mc.M_COST) / mr.M_REV 
  END as Margin
from filtertbl ft
left join mrev mr
on ft.AncillaryCode = mr.AncillaryCode
left join  mcost mc
on ft.AncillaryCode = mc.AncillaryCode
),


-----
----- Actual Bookings
-----
BundledCountTable AS (
 
 SELECT 
    PB.PNR_PAX_REF
    ,COUNT(CASE WHEN TP.BUNDLED_IN_FARE = false THEN 1 ELSE NULL END) AS OptionalCount
    ,SUM(CASE WHEN TP.BUNDLED_IN_FARE = true THEN 1 ELSE 0 END) AS BundledCount
FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB ON TPM.PASSENGER_ID = PB.PASSENGER_ID
AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE 
AND PB.dwh_current_flag = true and PB.dwh_delete_flag = FALSE     
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE  
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
AND TP.dwh_current_flag = true and TP.dwh_delete_flag = FALSE 
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C ON PB.FLIGHT_ID = C.FLIGHT_ID
AND C.dwh_current_flag = true and C.dwh_delete_flag = FALSE 
GROUP BY PB.PNR_PAX_REF
),

ActualBookings as
(
SELECT 

	 C.FLIGHT_NUMBER
	  ,PB.PNR_PAX_REF
	  ,ci.STATION_NAME
	  ,TPI.LEG_DATE
	  ,service_items.RFISC_CODE as AncillaryCode
	  , TPI.ANCILLARY_OFFER_CODE
      , TPI.ANCILLARY_OFFER_NAME
      ,date_format( TPI.SERVICE_START_TIME, 'HH:mm') AS ServiceStartTime
	  , Case 
	  when EMD_AMOUNT > 0 then 'Opt'
	  ELSE 'Bnd'
	  END as OptionalBundled
	  , CAST(TP.BUNDLED_IN_FARE AS INT) AS BundledFlag
      ,EMD_AMOUNT

  FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
  INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB on TPM.PASSENGER_ID = PB.PASSENGER_ID
  AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE 
  AND PB.dwh_current_flag = true and PB.dwh_delete_flag = FALSE   
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI on TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
  AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE   
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP on TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
  AND TP.dwh_current_flag = true and TP.dwh_delete_flag = FALSE     
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C on PB.FLIGHT_ID = C.FLIGHT_ID
  AND C.dwh_current_flag = true and C.dwh_delete_flag = FALSE    
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') ci on TPI.STATION_ID = ci.STATION_ID
  AND ci.dwh_current_flag = true and ci.dwh_delete_flag = FALSE    
  Left join IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items on service_items.SERVICE_ITEM_ID = TP.SERVICE_ITEM_ID	
  AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE      
  LEFT JOIN BundledCountTable ON PB.PNR_PAX_REF = BundledCountTable.PNR_PAX_REF
  where EMD_AMOUNT > 0

),


-----
----- Possible Bookings
-----
-- 1. Define journey boundaries
 JourneyBoundaries AS (
    SELECT
        FLIGHT_ID,
        MIN(LEG_DATE) AS FirstLegDate,
        MAX(LEG_DATE) AS LastLegDate
    FROM VFlight_Legs

    GROUP BY FLIGHT_ID
),

-- 2. Capture passenger bookings and flight leg details for one passenger
BookingDays AS (
    SELECT
        PB.PNR_PAX_REF,
        PB.PASSENGER_ID,
        PB.FLIGHT_ID,
        VI.FLIGHT_NUMBER,
        VI.LEG_DATE,
        VI.AIRPORT_NAME
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB
    INNER JOIN VFlight_Legs VI ON PB.FLIGHT_ID = VI.FLIGHT_ID
  AND PB.dwh_current_flag = true AND PB.dwh_delete_flag = FALSE      
),

-- 3. Capture ancillaries already booked (for that booking)
BookedAncillaries AS (
    SELECT
        PB.PNR_PAX_REF,
        C.FLIGHT_NUMBER,
        stations.STATION_NAME AS AIRPORT_NAME,
        TPI.LEG_DATE,
        service_items.RFISC_CODE AS AncillaryCode,
        TPI.ANCILLARY_OFFER_CODE,
        TPI.ANCILLARY_OFFER_NAME,
        date_format( TPI.SERVICE_START_TIME, 'HH:mm') AS ServiceStartTime,
        TPI.SERVICE_DURATION_HRS,
        SUBSTRING(DATEADD(MINUTE, TPI.SERVICE_DURATION_HRS * 60, CAST(TPI.SERVICE_START_TIME AS TIMESTAMP_NTZ)), 12, 5) AS ServiceEndTime,
        CAST(TP.BUNDLED_IN_FARE AS INT) AS BundledFlag,
        TPM.EMD_AMOUNT
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM ON PB.PASSENGER_ID = TPM.PASSENGER_ID AND TPM.REC_DELETED = false AND TPM.WAITLISTED = false
    AND PB.dwh_current_flag = true and PB.dwh_delete_flag = FALSE
    AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE          
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID AND TPI.REC_DELETED = false
    AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE   
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID AND TP.REC_DELETED = false
    AND TP.dwh_current_flag = true and TP.dwh_delete_flag = FALSE  
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C ON PB.FLIGHT_ID = C.FLIGHT_ID
    AND C.dwh_current_flag = true and C.dwh_delete_flag = FALSE   
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') AS stations ON TPI.STATION_ID = stations.STATION_ID
    AND stations.dwh_current_flag = true and stations.dwh_delete_flag = FALSE   
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items ON TP.SERVICE_ITEM_ID = service_items.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE  
),

-- 4. Count ancillaries per booking
AncillaryCounts AS (
    SELECT
        PB.PNR_PAX_REF,
        SUM(CASE WHEN TP.BUNDLED_IN_FARE = true THEN 1 ELSE 0 END) AS BundledCount,
        SUM(CASE WHEN TP.BUNDLED_IN_FARE = false THEN 1 ELSE 0 END) AS OptionalCount
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB ON TPM.PASSENGER_ID = PB.PASSENGER_ID
    AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE     
    AND PB.dwh_current_flag = true and PB.dwh_delete_flag = FALSE  
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
    AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE     
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
    AND TP.dwh_current_flag = true and TP.dwh_delete_flag = FALSE 
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C ON PB.FLIGHT_ID = C.FLIGHT_ID
    AND C.dwh_current_flag = true and C.dwh_delete_flag = FALSE  
    
    GROUP BY PB.PNR_PAX_REF
),

-- 5. Combine booking days with booked ancillaries and tag target bookings
TargetBookingsTable AS (
    SELECT
        BD.*,
        BT.AncillaryCode,
        BT.ANCILLARY_OFFER_CODE,
        BT.ANCILLARY_OFFER_NAME,
        BT.ServiceStartTime,
        BT.ServiceEndTime,
        BT.BundledFlag,
        BT.SERVICE_DURATION_HRS,
        BT.EMD_AMOUNT,
        CASE WHEN TC.BundledCount > 2 AND TC.OptionalCount >= 1 THEN 1 ELSE 0 END AS TargetBooking
    FROM BookingDays BD
    LEFT JOIN BookedAncillaries BT ON BD.PNR_PAX_REF = BT.PNR_PAX_REF AND BD.LEG_DATE = BT.LEG_DATE AND BD.AIRPORT_NAME = BT.AIRPORT_NAME
    JOIN AncillaryCounts TC ON BD.PNR_PAX_REF = TC.PNR_PAX_REF
    WHERE TC.BundledCount > 2 AND TC.OptionalCount >= 1 AND TC.OptionalCount < 3
),

-- 6. Identify dates where optional ancillaries are already booked
BookedOptionalDays AS (
    SELECT DISTINCT
        LEG_DATE,
        AIRPORT_NAME,
        PNR_PAX_REF
    FROM TargetBookingsTable
    WHERE BundledFlag = 0
),

-- 7. Get all optional ancillary offers for this journey
AncillaryOfferTable AS (
    SELECT
        flights.FLIGHT_NUMBER AS FlightNo,
        VStations.COUNTRY_NAME AS Country,
        CASE
            WHEN VStations.STATION_NAME LIKE '%In flight%' THEN 'In flight'
            WHEN flights.FLIGHT_NUMBER = 'SX0104' AND TPI.ANCILLARY_OFFER_CODE = 'NAPCTY01' AND VStations.STATION_NAME = 'Nashville' THEN 'Naples'
            WHEN flights.SCHED_DEPARTURE_DATE = '2016-10-02' AND ROUTE_MARKETING_NAME = 'East Coast Air Journey' AND TPI.ANCILLARY_OFFER_CODE LIKE '%YBGCTY01%' THEN 'Saguenay'
            ELSE VStations.STATION_NAME
        END AS Station,
        TPI.LEG_DATE AS ServiceDate,
        ancillary_products.BUNDLED_IN_FARE,
        service_items.RFISC_CODE AS AncillaryCode,
        TPI.ANCILLARY_OFFER_CODE,
        service_items.SERVICE_ITEM_NAME AS AncillaryName,
        LI.REF_CODE_NAME,
        TPI.SERVICE_DURATION_HRS,
        date_format( TPI.SERVICE_START_TIME, 'HH:mm') AS ServiceStartTime,
        COALESCE(TPI.INVENTORY_CAPACITY, COUNT(DISTINCT TPM.PASSENGER_ID)) AS CapacityPax,
        COUNT(DISTINCT TPM.PASSENGER_ID) AS BookedPax,
        TPRP.BASE_RETAIL_PRICE AS AncillaryPrice
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') AS ancillary_products ON TPI.ANCILLARY_PRODUCT_ID = ancillary_products.ANCILLARY_PRODUCT_ID
    AND TPI.dwh_current_flag = true and TPI.dwh_delete_flag = FALSE  
    AND ancillary_products.dwh_current_flag = true and ancillary_products.dwh_delete_flag = FALSE      
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') AS flights ON TPI.FLIGHT_ID = flights.FLIGHT_ID
    AND flights.dwh_current_flag = true and flights.dwh_delete_flag = FALSE    
    JOIN VStations ON TPI.STATION_ID = VStations.STATION_ID
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID AND TPM.REC_DELETED = false AND TPM.WAITLISTED = false
    AND TPM.dwh_current_flag = true and TPM.dwh_delete_flag = FALSE     
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items ON ancillary_products.SERVICE_ITEM_ID = service_items.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true and service_items.dwh_delete_flag = FALSE     
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_item_accounting') IMA ON service_items.SERVICE_ITEM_ID = IMA.SERVICE_ITEM_ID
    AND IMA.dwh_current_flag = true and IMA.dwh_delete_flag = FALSE     
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') LI ON IMA.GL_ACCOUNT_ID = LI.REF_CODE_ID
     AND LI.dwh_current_flag = true and LI.dwh_delete_flag = FALSE    
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_prices') TPRP ON service_items.SERVICE_ITEM_ID = TPRP.SERVICE_ITEM_ID AND TPRP.FARE_EFFECTIVE_FROM <= TPI.LEG_DATE AND TPRP.FARE_EFFECTIVE_TO >= TPI.LEG_DATE
     AND TPRP.dwh_current_flag = true and TPRP.dwh_delete_flag = FALSE    
    LEFT JOIN JourneyBoundaries CB ON CB.FLIGHT_ID = flights.FLIGHT_ID
    WHERE YEAR(flights.SCHED_DEPARTURE_DATE) > 2021
      AND TPI.REC_DELETED = false
      AND TPI.OFFER_STATUS_ID = 0
      AND TPI.ANCILLARY_CATEGORY_ID = 12
      AND ancillary_products.BUNDLED_IN_FARE = false
      AND TPI.SALE_ACTIVE = true
      AND TPI.LEG_DATE NOT IN (CB.FirstLegDate, CB.LastLegDate)
    GROUP BY
        flights.FLIGHT_NUMBER,
        VStations.COUNTRY_NAME,
        VStations.STATION_NAME,
        TPI.LEG_DATE,
        ancillary_products.BUNDLED_IN_FARE,
        service_items.RFISC_CODE,
        TPI.ANCILLARY_OFFER_CODE,
        service_items.SERVICE_ITEM_NAME,
        LI.REF_CODE_NAME,
        TPI.SERVICE_DURATION_HRS,
        TPI.SERVICE_START_TIME,
        TPI.INVENTORY_CAPACITY,
        TPRP.BASE_RETAIL_PRICE,
        flights.SCHED_DEPARTURE_DATE,
        flights.ROUTE_MARKETING_NAME
),

-- 8. Final result: only optional ancillary offers where no optional ancillary is already booked on that date
PossibleBookings AS 
(
SELECT 
    S.FlightNo,
    S.ServiceDate,
    S.Country,
    S.Station,
    'Opt' AS OfferType,
    S.AncillaryCode,
    S.ANCILLARY_OFFER_CODE,
    S.AncillaryName,
    S.REF_CODE_NAME,
    S.SERVICE_DURATION_HRS,
    S.ServiceStartTime,
    S.CapacityPax,
    S.BookedPax,
    S.AncillaryPrice,
    TB.PNR_PAX_REF,
	  m.Margin

FROM AncillaryOfferTable S
JOIN TargetBookingsTable TB ON
    S.FlightNo = TB.FLIGHT_NUMBER 
	AND S.Station = TB.AIRPORT_NAME 
	AND S.ServiceDate = TB.LEG_DATE 
	AND (S.CapacityPax - S.BookedPax) > 5
	LEFT JOIN margin m
	ON s.AncillaryCode = m.AncillaryCode
WHERE TB.PNR_PAX_REF IS NOT NULL
  AND S.REF_CODE_NAME = 'Ancillary Optional'
  AND NOT EXISTS (
      SELECT 1 FROM BookedOptionalDays BOD
      WHERE BOD.LEG_DATE = S.ServiceDate
        AND BOD.AIRPORT_NAME = S.Station
        AND BOD.PNR_PAX_REF = TB.PNR_PAX_REF
		)
),

AvgPriceTime AS (
SELECT  
ab.PNR_PAX_REF,
 AVG(ab.EMD_AMOUNT) AS AvgPrice,
 AVG( DATEPART('HOUR', ab.ServiceStartTime)  +  ( DATEPART('MINUTE', ab.ServiceStartTime) / 60.0 ) ) AS AvgTime

FROM ActualBookings ab
INNER JOIN PossibleBookings pb
ON ab.PNR_PAX_REF = pb.PNR_PAX_REF
WHERE ab.OptionalBundled = 'Opt'
GROUP BY ab.PNR_PAX_REF
),

MinPriceTimeDiff AS (
SELECT
pb.*,
ABS(pb.AncillaryPrice - ap.AvgPrice) AS MinPriceDiff,
ABS(DATEPART('HOUR', pb.ServiceStartTime) + (DATEPART('MINUTE', pb.ServiceStartTime) / 60.0 ) - ap.AvgTime ) AS MinDepartureDiff
FROM PossibleBookings pb
LEFT JOIN AvgPriceTime ap
ON pb.PNR_PAX_REF = ap.PNR_PAX_REF
),

MinMax AS 
(
SELECT 
PNR_PAX_REF,
min(MinPriceDiff) AS MinP,
max(MinPriceDiff) AS MaxP,
min(MinDepartureDiff) AS MinT,
max(MinDepartureDiff) AS MaxT

FROM MinPriceTimeDiff
GROUP BY PNR_PAX_REF
),


NormPriceTimeDiff as (
SELECT 

md.PNR_PAX_REF,
md.AncillaryCode,
md.ANCILLARY_OFFER_CODE,
mm.minP, 
mm.maxP,
md.MinPriceDiff,
  CASE 
    WHEN mm.maxP - mm.minP = 0 THEN NULL
    ELSE (md.MinPriceDiff - mm.minP) / (mm.maxP - mm.minP) 
  END AS NormPriceDiff,
mm.minT ,mm.maxT ,
md.MinDepartureDiff,
  CASE 
     WHEN mm.maxT - mm.minT = 0 THEN NULL
	 ELSE (md.MinDepartureDiff - mm.MinT) / (mm.maxT - mm.minT) 
  END AS NormTimeDiff 

FROM MinPriceTimeDiff md
LEFT JOIN MinMax mm
ON md.PNR_PAX_REF = mm.PNR_PAX_REF
)

SELECT 
DISTINCT
    pb.FlightNo,
    pb.ServiceDate,
    pb.Country,
    pb.Station,
    pb.OfferType,
    pb.AncillaryCode,
    pb.ANCILLARY_OFFER_CODE,
    pb.AncillaryName,
    pb.REF_CODE_NAME,
    pb.SERVICE_DURATION_HRS,
    pb.ServiceStartTime,
    pb.CapacityPax,
    pb.BookedPax,
    pb.AncillaryPrice,
    pb.PNR_PAX_REF,
	  pb.Margin,
NormPriceDiff,
NormTimeDiff,
NormPriceDiff * 0.8 + NormTimeDiff * 0.2 + (1 - m.Margin)  * 0.5 as OfferRankScore

FROM PossibleBookings pb
LEFT JOIN NormPriceTimeDiff nd
ON pb.PNR_PAX_REF = nd.PNR_PAX_REF
AND pb.AncillaryCode = nd.AncillaryCode
AND pb.ANCILLARY_OFFER_CODE = nd.ANCILLARY_OFFER_CODE
LEFT JOIN margin m
ON pb.AncillaryCode = m.AncillaryCode

-- COMMAND ----------

-- DBTITLE 1,OfferScoreRanking

CREATE TEMPORARY VIEW OfferScoreRanking 
AS
SELECT DISTINCT 
PNR_PAX_REF as PnrPaxRef,
LEFT(PnrPaxRef,7) as PNR,
RIGHT(PnrPaxRef,1) as PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore

FROM AncillaryOfferScoreRanking tr
WHERE ServiceDate > GETDATE()
AND OfferRankScore IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Top3Offers
CREATE OR REFRESH MATERIALIZED VIEW Top3Offers
AS 

With OnePerDay as (
select 
 RANK() OVER 
     (PARTITION BY PnrPaxRef,ServiceDate ORDER BY OfferRankScore ASC) AS Rank  ,* 
FROM OfferScoreRanking 
),

RankPerDay AS (
SELECT  *
FROM OnePerDay
WHERE Rank = 1
),

DedupTourCode AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef,PNR,PAX_SEQ,FlightNo,AncillaryCode ORDER BY OfferRankScore,ServiceDate ) AS RowDedup 
FROM RankPerDay 
),

RankedOverall AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef ORDER BY OfferRankScore,ServiceDate ) AS RowNum 
-- FROM RankPerDay
FROM DedupTourCode
WHERE RowDedup = 1
)

SELECT 
PnrPaxRef,
PNR,
PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore,
RowNum
FROM RankedOverall
WHERE RowNum <= 3



-- COMMAND ----------

-- DBTITLE 1,Top6Offers
CREATE OR REFRESH MATERIALIZED VIEW Top6Offers
AS 

With OnePerDay as (
select 
 RANK() OVER 
     (PARTITION BY PnrPaxRef,ServiceDate ORDER BY OfferRankScore ASC) AS Rank  ,* 
FROM OfferScoreRanking
),

RankPerDay AS (
SELECT  *
FROM OnePerDay
WHERE Rank = 1
),

DedupTourCode AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef,PNR,PAX_SEQ,FlightNo,AncillaryCode ORDER BY OfferRankScore,ServiceDate ) AS RowDedup 
FROM RankPerDay 
),

RankedOverall AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef ORDER BY OfferRankScore,ServiceDate ) AS RowNum 
FROM DedupTourCode
WHERE RowDedup = 1
)

SELECT 
PnrPaxRef,
PNR,
PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore,
ROW_NUMBER() OVER (PARTITION BY PnrPaxRef ORDER BY OfferRankScore,ServiceDate ) AS RowNum 
FROM RankedOverall
WHERE RowNum <= 6



-- COMMAND ----------

-- DBTITLE 1,Unique_Email_Consent
CREATE TEMPORARY VIEW Unique_Email_Consent
AS 

WITH latest_consent AS (
SELECT 
EMAIL_ADDRESS,
OPT_OUT_DATE,
 ROW_NUMBER() OVER 
     (PARTITION BY EMAIL_ADDRESS ORDER BY OPT_OUT_DATE DESC) AS RowNum
FROM IDENTIFIER(:raw_catalog || '.' || :raw_feature_schema || '.email_consents') AS email_consents 
WHERE 1=1
AND dwh_current_flag = true AND dwh_delete_flag = false
)

SELECT 
EMAIL_ADDRESS,
OPT_OUT_DATE

FROM latest_consent
WHERE RowNum = 1

-- COMMAND ----------

-- DBTITLE 1,BND_Ancillary

CREATE OR REFRESH MATERIALIZED VIEW BND_Ancillary
AS

WITH pax1 AS (
  SELECT t1.PnrPaxRef,
t1.PNR,
t1.PAX_SEQ,
t1.AncillaryCode AS Ancillary_Code1,
t2.AncillaryCode AS Ancillary_Code2,
t3.AncillaryCode AS Ancillary_Code3,
p.FIRST_NAME,
p.LAST_NAME,
p.email
 FROM Top3Offers t1
INNER JOIN Top3Offers t2
ON t1.PnrPaxRef = t2.PnrPaxRef AND t2.PAX_SEQ = 1 AND t2.RowNum = 2
INNER JOIN Top3Offers t3
ON t1.PnrPaxRef = t3.PnrPaxRef AND t3.PAX_SEQ = 1 AND t3.RowNum = 3
LEFT JOIN (
  SELECT 
  PNR,
  cast(PAX_SEQ AS STRING) as PAX_SEQ,
  CONCAT(PNR, cast(PAX_SEQ AS STRING)) AS PnrPaxRef,
  FIRST_NAME,
  LAST_NAME,
  email 
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.pnr_passengers') AS pnr_passengers
    WHERE dwh_current_flag = true and dwh_delete_flag = false
	) p
ON t1.PnrPaxRef = p.PnrPaxRef
WHERE  t1.RowNum = 1
AND t1.PAX_SEQ = 1 
),

pax2 AS (

SELECT t1.PnrPaxRef,
t2.PnrPaxRef,
t1.PNR,
t1.PAX_SEQ,
t1.AncillaryCode AS Ancillary_Code4,
t2.AncillaryCode AS Ancillary_Code5,
t3.AncillaryCode AS Ancillary_Code6,
p.FIRST_NAME,
p.LAST_NAME,
p.email
FROM Top6Offers t1
INNER JOIN Top6Offers t2
ON t1.PnrPaxRef = t2.PnrPaxRef AND t2.PAX_SEQ = 2 AND t2.RowNum = 5
INNER JOIN Top6Offers t3
ON t1.PnrPaxRef = t3.PnrPaxRef AND t3.PAX_SEQ = 2 AND t3.RowNum = 6
LEFT JOIN (
  SELECT 
  PNR,
  cast(PAX_SEQ AS STRING) as PAX_SEQ,
  CONCAT(PNR, cast(PAX_SEQ AS STRING)) AS PnrPaxRef,
  FIRST_NAME,
  LAST_NAME,  
  email 
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.pnr_passengers') AS pnr_passengers
    WHERE dwh_current_flag = true and dwh_delete_flag = false
	) p
ON t1.PnrPaxRef = p.PnrPaxRef
WHERE  t1.RowNum = 4
AND t1.PAX_SEQ = 2 
),

allbooking AS (
SELECT 
DISTINCT
t.PNR,
t.FlightNo,
s.POS_OFFICE,
s.CURRENCY_CODE,
o.CURRENCY_NAME

FROM Top3Offers t
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.ticket_sales') s
ON t.PNR = s.PNR
AND s.dwh_current_flag = true AND s.dwh_delete_flag = FALSE 
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.flight_schedule') c
ON s.FLIGHT_NO = c.FLIGHT_NO 
AND c.dwh_current_flag = true AND c.dwh_delete_flag = false
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.office_currencies') o
ON s.POS_OFFICE = o.POS_OFFICE
AND s.CURRENCY_CODE = o.CURRENCY_CODE
AND o.dwh_current_flag = true AND o.dwh_delete_flag = FALSE 

WHERE ( c.DEPARTURE_DATE >= date_add(DAY, CAST(:FromOffset AS INT),current_date) 
AND c.DEPARTURE_DATE <= date_add(DAY, CAST(:ToOffset AS INT),current_date) ) 
AND s.TICKET_STATUS <> 'X'
)

SELECT 
CONCAT('ANCILLARY-', p1.PNR) as Record_ID,
a.FlightNo AS Flight_Number,
a.PNR AS PNR_ID,
c.DEPARTURE_DATE AS Departure_Date,
c.ROUTE_NAME AS Route_Name,
p1.FIRST_NAME AS Pax1_FirstName,
p1.LAST_NAME AS Pax1_LastName,
p2.FIRST_NAME AS Pax2_FirstName,
p2.LAST_NAME AS Pax2_LastName,
p1.email AS Pax1_Email1,
'NULL' AS Pax1_Email2,
p2.email AS Pax2_Email1,
'NULL' AS Pax2_Email2,
p1.Ancillary_Code1,
p1.Ancillary_Code2,
p1.Ancillary_Code3,
p2.Ancillary_Code4,
p2.Ancillary_Code5,
p2.Ancillary_Code6,
'Y' AS NewRecord_Flag,
CURRENT_TIMESTAMP AS Timestamp,
CASE e1.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax1_Email1_Flag,
CASE e1.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax1_Email2_Flag,
CASE e2.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax2_Email1_Flag,
CASE e2.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax2_Email2_Flag,
a.POS_OFFICE,
a.CURRENCY_CODE,
a.CURRENCY_NAME,
'OPT' AS Ancillary_Offer_Type,
'AIR' AS Product_Line

FROM allbooking a
INNER JOIN pax1 p1
ON a.PNR = p1.PNR
LEFT JOIN pax2 p2
ON a.PNR = p2.PNR
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.flight_schedule') c
ON a.FlightNo = c.FLIGHT_NO 
AND c.dwh_current_flag = true AND c.dwh_delete_flag = false
LEFT JOIN Unique_Email_Consent e1
ON UPPER(p1.email) = e1.EMAIL_ADDRESS
LEFT JOIN Unique_Email_Consent e2
ON UPPER(p2.email) = e2.EMAIL_ADDRESS

-- COMMAND ----------

-- DBTITLE 1,AncillaryOfferScoreRankingBundled
CREATE OR REFRESH MATERIALIZED VIEW AncillaryOfferScoreRankingBundled 
AS
-----
----- Actual Bookings
-----

With BundledCountTable 
AS (
 
 SELECT 
    PB.PNR_PAX_REF
    ,COUNT(CASE WHEN TP.BUNDLED_IN_FARE = false THEN 1 ELSE NULL END) AS OptionalCount
    ,SUM(CASE WHEN TP.BUNDLED_IN_FARE = true THEN 1 ELSE 0 END) AS BundledCount

FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB 
  ON TPM.PASSENGER_ID = PB.PASSENGER_ID
  AND PB.dwh_current_flag = true 
  AND PB.dwh_delete_flag = false
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI 
  ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
  AND TPI.dwh_current_flag = true
  AND TPI.dwh_delete_flag = false
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP 
  ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
  AND TP.dwh_current_flag = true
  AND TP.dwh_delete_flag = false
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C 
  ON PB.FLIGHT_ID = C.FLIGHT_ID
  AND C.dwh_current_flag = true
  AND C.dwh_delete_flag = false

WHERE 1=1
AND TPM.dwh_current_flag = true
AND TPM.dwh_delete_flag = false

GROUP BY PB.PNR_PAX_REF
),

ActualBookings as
(
SELECT 
       
	 C.FLIGHT_NUMBER
	  ,PB.PNR_PAX_REF
	  ,Station.STATION_NAME
	  ,TPI.LEG_DATE
	  ,service_items.RFISC_CODE as AncillaryCode
	  , TPI.ANCILLARY_OFFER_CODE
    , TPI.ANCILLARY_OFFER_NAME
    ,date_format( TPI.SERVICE_START_TIME, 'HH:mm') as ServiceStartTime
	  , Case 
	  when EMD_AMOUNT > 0 then 'Opt'
	  ELSE 'Bnd'
	  END as OptBundled
	  , CAST(TP.BUNDLED_IN_FARE AS INT) AS BundledFlag

  FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
  INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB 
    on TPM.PASSENGER_ID = PB.PASSENGER_ID
    AND PB.dwh_current_flag = true 
    AND PB.dwh_delete_flag = false  
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI 
    on TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
    AND TPI.dwh_current_flag = true
    AND TPI.dwh_delete_flag = false
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP 
    on TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
    AND TP.dwh_current_flag = true
    AND TP.dwh_delete_flag = false    
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C 
    on PB.FLIGHT_ID = C.FLIGHT_ID
    AND C.dwh_current_flag = true
    AND C.dwh_delete_flag = false  
  LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.stations') Station 
    on TPI.STATION_ID = Station.STATION_ID
    AND Station.dwh_current_flag = true
    AND Station.dwh_delete_flag = false  
  Left join IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items 
    on service_items.SERVICE_ITEM_ID = TP.SERVICE_ITEM_ID
    AND service_items.dwh_current_flag = true
    AND service_items.dwh_delete_flag = false  	
  LEFT JOIN BundledCountTable 
    ON PB.PNR_PAX_REF = BundledCountTable.PNR_PAX_REF

  WHERE 1=1
  AND TPM.dwh_current_flag = true
  AND TPM.dwh_delete_flag = false
  AND OptionalCount = 0

  -- order by 
  -- PB.PNR_PAX_REF, TPI.LEG_DATE

),

-----
----- Possible Bookings
-----

-- 1. Define journey boundaries
JourneyBoundaries AS (
    SELECT
        FLIGHT_ID,
        MIN(LEG_DATE) AS FirstLegDate,
        MAX(LEG_DATE) AS LastLegDate
    FROM VFlight_Legs
    GROUP BY FLIGHT_ID
),

-- 2. Get booking-day combinations for passengers
BookingDays AS (
    SELECT
        PB.PNR_PAX_REF,
        PB.PASSENGER_ID,
        PB.FLIGHT_ID,
        VI.FLIGHT_NUMBER,
        VI.LEG_DATE,
        VI.AIRPORT_NAME
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB
    INNER JOIN VFlight_Legs VI 
      ON PB.FLIGHT_ID = VI.FLIGHT_ID
    WHERE 1=1
    AND PB.dwh_current_flag = true 
    AND PB.dwh_delete_flag = false     
),

-- 3. Count ancillaries per booking to filter down to “bundled only” passengers
AncillaryCounts AS (
    SELECT
        PB.PNR_PAX_REF,
        SUM(CASE WHEN TP.BUNDLED_IN_FARE = true THEN 1 ELSE 0 END) AS BundledCount,
        SUM(CASE WHEN TP.BUNDLED_IN_FARE = false THEN 1 ELSE 0 END) AS OptionalCount
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.passenger_bookings') PB 
      ON TPM.PASSENGER_ID = PB.PASSENGER_ID
      AND PB.dwh_current_flag = true 
      AND PB.dwh_delete_flag = false       
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI 
      ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID
      AND TPI.dwh_current_flag = true
      AND TPI.dwh_delete_flag = false      
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP 
      ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
      AND TP.dwh_current_flag = true
      AND TP.dwh_delete_flag = false       
    WHERE 1=1
    AND TPM.dwh_current_flag = true
    AND TPM.dwh_delete_flag = false
    GROUP BY PB.PNR_PAX_REF
),

-- 4. Filter target passengers who booked only bundled ancillaries
TargetBookingsTable AS (
    SELECT
        BD.*
    FROM BookingDays BD
    JOIN AncillaryCounts TC 
    ON BD.PNR_PAX_REF = TC.PNR_PAX_REF
    WHERE TC.BundledCount >= 0 
    AND TC.OptionalCount = 0
),

-- 5. All eligible optional ancillary offers for the journey
AncillaryOfferTable AS (
    SELECT
        C.FLIGHT_NUMBER AS FlightNo,
        VStations.COUNTRY_NAME AS Country,
        CASE
            WHEN VStations.STATION_NAME LIKE '%In flight%' THEN 'In flight'
            WHEN C.FLIGHT_NUMBER = 'SX0104' AND TPI.ANCILLARY_OFFER_CODE = 'NAPCTY01' AND VStations.STATION_NAME = 'Nashville' THEN 'Naples'
            WHEN C.SCHED_DEPARTURE_DATE = '2016-10-02' AND ROUTE_MARKETING_NAME = 'East Coast Air Journey' AND TPI.ANCILLARY_OFFER_CODE LIKE '%YBGCTY01%' THEN 'Saguenay'
            ELSE VStations.STATION_NAME
        END AS Station,
        TPI.LEG_DATE AS ServiceDate,
        service_items.RFISC_CODE AS AncillaryCode,
        TPI.ANCILLARY_OFFER_CODE,
        service_items.SERVICE_ITEM_NAME AS AncillaryName,
        LI.REF_CODE_NAME,
        TPI.SERVICE_DURATION_HRS,
        -- CONVERT(VARCHAR(5), TPI.SERVICE_START_TIME, 108) AS ServiceStartTime,
        date_format( TPI.SERVICE_START_TIME, 'HH:mm') AS ServiceStartTime,
        COALESCE(TPI.INVENTORY_CAPACITY, COUNT(DISTINCT TPM.PASSENGER_ID)) AS CapacityPax,
        COUNT(DISTINCT TPM.PASSENGER_ID) AS BookedPax,
        TPRP.BASE_RETAIL_PRICE AS AncillaryPrice

    FROM IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_offers') TPI
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_products') TP
      ON TPI.ANCILLARY_PRODUCT_ID = TP.ANCILLARY_PRODUCT_ID
      AND TP.dwh_current_flag = true
      AND TP.dwh_delete_flag = false   
    JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.flights') C
      ON TPI.FLIGHT_ID = C.FLIGHT_ID
      AND C.dwh_current_flag = true
      AND C.dwh_delete_flag = false    
    JOIN VStations 
      ON TPI.STATION_ID = VStations.STATION_ID
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_sales') TPM 
      ON TPM.ANCILLARY_OFFER_ID = TPI.ANCILLARY_OFFER_ID 
        AND TPM.REC_DELETED = false
        AND TPM.WAITLISTED = FALSE
        AND TPM.dwh_current_flag = true
        AND TPM.dwh_delete_flag = false   
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_items') AS service_items 
      ON TP.SERVICE_ITEM_ID = service_items.SERVICE_ITEM_ID
      AND service_items.dwh_current_flag = true
      AND service_items.dwh_delete_flag = false  
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.service_item_accounting') IMA 
      ON service_items.SERVICE_ITEM_ID = IMA.SERVICE_ITEM_ID
      AND IMA.dwh_current_flag = true
      AND IMA.dwh_delete_flag = false      
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ref_codes') LI 
      ON IMA.GL_ACCOUNT_ID = LI.REF_CODE_ID
      AND LI.dwh_current_flag = true
      AND LI.dwh_delete_flag = false      
    LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_main_schema || '.ancillary_prices') TPRP 
      ON service_items.SERVICE_ITEM_ID = TPRP.SERVICE_ITEM_ID 
      AND TPRP.FARE_EFFECTIVE_FROM <= TPI.LEG_DATE 
      AND TPRP.FARE_EFFECTIVE_TO >= TPI.LEG_DATE
      AND TPRP.dwh_current_flag = true
      AND TPRP.dwh_delete_flag = false       
    LEFT JOIN JourneyBoundaries CB 
      ON CB.FLIGHT_ID = C.FLIGHT_ID
    WHERE 1=1
      AND TPI.dwh_current_flag = true
      AND TPI.dwh_delete_flag = false
      AND YEAR(C.SCHED_DEPARTURE_DATE) > 2023
      AND TPI.REC_DELETED = false
      AND TPI.OFFER_STATUS_ID = 0
      AND TPI.ANCILLARY_CATEGORY_ID = 12
      AND TP.BUNDLED_IN_FARE = false
      AND TPI.SALE_ACTIVE = true
      AND TPI.LEG_DATE NOT IN (CB.FirstLegDate, CB.LastLegDate)
      AND TPRP.BASE_RETAIL_PRICE < 250
    GROUP BY
        C.FLIGHT_NUMBER,
        VStations.COUNTRY_NAME,
        VStations.STATION_NAME,
        TPI.LEG_DATE,
        service_items.RFISC_CODE,
        TPI.ANCILLARY_OFFER_CODE,
        service_items.SERVICE_ITEM_NAME,
        LI.REF_CODE_NAME,
        TPI.SERVICE_DURATION_HRS,
        TPI.SERVICE_START_TIME,
        TPI.INVENTORY_CAPACITY,
        TPRP.BASE_RETAIL_PRICE,
        C.SCHED_DEPARTURE_DATE,
        C.ROUTE_MARKETING_NAME
),

-- 6. Final recommendation: optional ancillaries for qualified passengers (with availability)
PossibleBookings AS 
(
SELECT
    S.FlightNo,
    S.ServiceDate,
    S.Country,
    S.Station,
    'Bnd' AS OfferType,
    S.AncillaryCode,
    S.ANCILLARY_OFFER_CODE,
    S.AncillaryName,
    S.REF_CODE_NAME,
    S.SERVICE_DURATION_HRS,
    S.ServiceStartTime,
    S.CapacityPax,
    S.BookedPax,
    S.AncillaryPrice,
    TB.PNR_PAX_REF,
    S.CapacityPax - S.BookedPax AS SeatsRemaining
FROM AncillaryOfferTable S
JOIN TargetBookingsTable TB ON
    S.FlightNo = TB.FLIGHT_NUMBER AND
    S.Station = TB.AIRPORT_NAME AND
    S.ServiceDate = TB.LEG_DATE
WHERE
    (S.CapacityPax - S.BookedPax) > 5
    AND TB.PNR_PAX_REF IS NOT NULL
    AND S.REF_CODE_NAME = 'Ancillary Optional'

),

AvgTime AS (
SELECT  
ab.PNR_PAX_REF,
 AVG( DATEPART('HOUR', ab.ServiceStartTime)  +  ( DATEPART('MINUTE', ab.ServiceStartTime) / 60.0 ) ) AS AvgTime

FROM ActualBookings ab
INNER JOIN PossibleBookings pb
ON ab.PNR_PAX_REF = pb.PNR_PAX_REF
WHERE ab.OptBundled = 'Bnd'
GROUP BY ab.PNR_PAX_REF
),

MinTimeDiff AS (
SELECT
pb.*,
ABS(DATEPART('HOUR', pb.ServiceStartTime) + (DATEPART('MINUTE', pb.ServiceStartTime) / 60.0 ) - ap.AvgTime ) AS MinDepartureDiff
FROM PossibleBookings pb
LEFT JOIN AvgTime ap
ON pb.PNR_PAX_REF = ap.PNR_PAX_REF
)

SELECT 
DISTINCT
    pb.FlightNo,
    pb.ServiceDate,
    pb.Country,
    pb.Station,
    pb.OfferType,
    pb.AncillaryCode,
    pb.ANCILLARY_OFFER_CODE,
    pb.AncillaryName,
    pb.REF_CODE_NAME,
    pb.SERVICE_DURATION_HRS,
    pb.ServiceStartTime,
    pb.CapacityPax,
    pb.BookedPax,
    pb.AncillaryPrice,
    pb.PNR_PAX_REF,
	  pb.SeatsRemaining,
    md.MinDepartureDiff,
    (md.MinDepartureDiff * 0.1) + (1 / pb.SeatsRemaining * 0.8) + (pb.AncillaryPrice * 0.001)  as OfferRankScore


FROM PossibleBookings pb
LEFT JOIN MinTimeDiff md
ON pb.PNR_PAX_REF = md.PNR_PAX_REF
AND pb.AncillaryCode = md.AncillaryCode
AND pb.ANCILLARY_OFFER_CODE = md.ANCILLARY_OFFER_CODE



-- COMMAND ----------

-- DBTITLE 1,OfferScoreRankingBundled

CREATE TEMPORARY VIEW OfferScoreRankingBundled 
AS
SELECT DISTINCT 
PNR_PAX_REF as PnrPaxRef,
LEFT(PnrPaxRef,7) as PNR,
RIGHT(PnrPaxRef,1) as PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore

FROM AncillaryOfferScoreRankingBundled tr
WHERE ServiceDate > GETDATE()
AND OfferRankScore IS NOT NULL

-- COMMAND ----------

-- DBTITLE 1,Top3OffersBundled
CREATE OR REFRESH MATERIALIZED VIEW Top3OffersBundled 
AS 

With OnePerDay as (
select 
 RANK() OVER 
     (PARTITION BY PnrPaxRef,ServiceDate ORDER BY OfferRankScore ASC) AS Rank  ,* 
FROM OfferScoreRankingBundled  
),

RankPerDay AS (
SELECT  *
FROM OnePerDay
WHERE Rank = 1
),

DedupTourCode AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef,PNR,PAX_SEQ,FlightNo,AncillaryCode ORDER BY OfferRankScore,ServiceDate ) AS RowDedup 
FROM RankPerDay 
),

RankedOverall AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef ORDER BY OfferRankScore,ServiceDate ) AS RowNum 
-- FROM RankPerDay
FROM DedupTourCode
WHERE RowDedup = 1
)

SELECT 
PnrPaxRef,
PNR,
PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore,
RowNum
FROM RankedOverall
WHERE RowNum <= 3

-- COMMAND ----------

-- DBTITLE 1,Top6OffersBundled
CREATE OR REFRESH MATERIALIZED VIEW Top6OffersBundled  
AS 

With OnePerDay as (
select 
 RANK() OVER 
     (PARTITION BY PnrPaxRef,ServiceDate ORDER BY OfferRankScore ASC) AS Rank  ,* 
FROM OfferScoreRankingBundled
),

RankPerDay AS (
SELECT  *
FROM OnePerDay
WHERE Rank = 1
),

DedupTourCode AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef,PNR,PAX_SEQ,FlightNo,AncillaryCode ORDER BY OfferRankScore,ServiceDate ) AS RowDedup 
FROM RankPerDay 
),

RankedOverall AS (
SELECT 
*
,ROW_NUMBER() OVER (PARTITION BY PnrPaxRef ORDER BY OfferRankScore,ServiceDate ) AS RowNum 
FROM DedupTourCode
WHERE RowDedup = 1
)

SELECT 
PnrPaxRef,
PNR,
PAX_SEQ,
FlightNo,
ServiceDate,
AncillaryCode,
OfferRankScore,
RowNum
FROM RankedOverall
WHERE RowNum <= 6

-- COMMAND ----------

-- DBTITLE 1,BND_Ancillary_Bundled
CREATE OR REFRESH MATERIALIZED VIEW BND_Ancillary_Bundled
AS

WITH pax1 AS (
  SELECT t1.PnrPaxRef,
t1.PNR,
t1.PAX_SEQ,
t1.AncillaryCode AS Ancillary_Code1,
t2.AncillaryCode AS Ancillary_Code2,
t3.AncillaryCode AS Ancillary_Code3,
p.FIRST_NAME,
p.LAST_NAME,
p.email
 FROM Top3OffersBundled t1
INNER JOIN Top3OffersBundled t2
ON t1.PnrPaxRef = t2.PnrPaxRef AND t2.PAX_SEQ = 1 AND t2.RowNum = 2
INNER JOIN Top3OffersBundled t3
ON t1.PnrPaxRef = t3.PnrPaxRef AND t3.PAX_SEQ = 1 AND t3.RowNum = 3
LEFT JOIN (
  SELECT 
  PNR,
  cast(PAX_SEQ AS STRING) as PAX_SEQ,
  CONCAT(PNR, cast(PAX_SEQ AS STRING)) AS PnrPaxRef,
  FIRST_NAME,
  LAST_NAME,
  email 
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.pnr_passengers') AS pnr_passengers
    WHERE dwh_current_flag = true and dwh_delete_flag = false
	) p
ON t1.PnrPaxRef = p.PnrPaxRef
WHERE  t1.RowNum = 1
AND t1.PAX_SEQ = 1 
),

pax2 AS (

SELECT t1.PnrPaxRef,
t2.PnrPaxRef,
t1.PNR,
t1.PAX_SEQ,
t1.AncillaryCode AS Ancillary_Code4,
t2.AncillaryCode AS Ancillary_Code5,
t3.AncillaryCode AS Ancillary_Code6,
p.FIRST_NAME,
p.LAST_NAME,
p.email

FROM Top6OffersBundled t1
INNER JOIN Top6OffersBundled t2
ON t1.PnrPaxRef = t2.PnrPaxRef AND t2.PAX_SEQ = 2 AND t2.RowNum = 5
INNER JOIN Top6OffersBundled t3
ON t1.PnrPaxRef = t3.PnrPaxRef AND t3.PAX_SEQ = 2 AND t3.RowNum = 6
LEFT JOIN (
  SELECT 
  PNR,
  cast(PAX_SEQ AS STRING) as PAX_SEQ,
  CONCAT(PNR, cast(PAX_SEQ AS STRING)) AS PnrPaxRef,
  FIRST_NAME,
  LAST_NAME,  
  email 
    FROM IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.pnr_passengers') AS pnr_passengers
    WHERE dwh_current_flag = true and dwh_delete_flag = false
	) p
ON t1.PnrPaxRef = p.PnrPaxRef
WHERE  t1.RowNum = 4
AND t1.PAX_SEQ = 2 
),

allbooking AS (
SELECT 
DISTINCT
t.PNR,
t.FlightNo,
s.POS_OFFICE,
s.CURRENCY_CODE,
o.CURRENCY_NAME

FROM Top3OffersBundled t
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.ticket_sales') s
ON t.PNR = s.PNR
AND s.dwh_current_flag = true AND s.dwh_delete_flag = FALSE 
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.flight_schedule') c
ON s.FLIGHT_NO = c.FLIGHT_NO 
AND c.dwh_current_flag = true AND c.dwh_delete_flag = false
LEFT JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.office_currencies') o
ON s.POS_OFFICE = o.POS_OFFICE
AND s.CURRENCY_CODE = o.CURRENCY_CODE
AND o.dwh_current_flag = true AND o.dwh_delete_flag = FALSE 

WHERE ( c.DEPARTURE_DATE >= date_add(DAY, CAST(:FromOffset AS INT),current_date) 
AND c.DEPARTURE_DATE <= date_add(DAY, CAST(:ToOffset AS INT),current_date) )
AND s.TICKET_STATUS <> 'X'
)

SELECT 
CONCAT('ANCILLARY-', p1.PNR) as Record_ID,
a.FlightNo AS Flight_Number,
a.PNR AS PNR_ID,
c.DEPARTURE_DATE AS Departure_Date,
c.ROUTE_NAME AS Route_Name,
p1.FIRST_NAME AS Pax1_FirstName,
p1.LAST_NAME AS Pax1_LastName,
p2.FIRST_NAME AS Pax2_FirstName,
p2.LAST_NAME AS Pax2_LastName,
p1.email AS Pax1_Email1,
'NULL' AS Pax1_Email2,
p2.email AS Pax2_Email1,
'NULL' AS Pax2_Email2,
p1.Ancillary_Code1,
p1.Ancillary_Code2,
p1.Ancillary_Code3,
p2.Ancillary_Code4,
p2.Ancillary_Code5,
p2.Ancillary_Code6,
'Y' AS NewRecord_Flag,
CURRENT_TIMESTAMP AS Timestamp,
CASE e1.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax1_Email1_Flag,
CASE e1.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax1_Email2_Flag,
CASE e2.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax2_Email1_Flag,
CASE e2.OPT_OUT_DATE
  WHEN '1900-01-01' THEN 'Y'
  ELSE 'N'
END AS OptIn_Pax2_Email2_Flag,
a.POS_OFFICE,
a.CURRENCY_CODE,
a.CURRENCY_NAME,
'BND' AS Ancillary_Offer_Type,
'AIR' AS Product_Line

FROM allbooking a
INNER JOIN pax1 p1
ON a.PNR = p1.PNR
LEFT JOIN pax2 p2
ON a.PNR = p2.PNR
INNER JOIN IDENTIFIER(:raw_catalog || '.' || :raw_branch_schema || '.flight_schedule') c
ON a.FlightNo = c.FLIGHT_NO 
AND c.dwh_current_flag = true AND c.dwh_delete_flag = false
LEFT JOIN Unique_Email_Consent e1
ON UPPER(p1.email) = e1.EMAIL_ADDRESS
LEFT JOIN Unique_Email_Consent e2
ON UPPER(p2.email) = e2.EMAIL_ADDRESS