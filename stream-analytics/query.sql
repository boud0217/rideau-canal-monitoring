SELECT 
	IoTHub.ConnectionDeviceId AS ConnectionDeviceId,
	AVG(ice_thickness) AS AvgIceThickness,
	MIN(ice_thickness) AS MinIceThickness,
	MAX(ice_thickness) AS MaxIceThickness,
	AVG(surface_temperature) AS AvgSurfaceTemperature,
	MIN(surface_temperature) AS MinSurfaceTemperature,
	MAX(surface_temperature) AS MaxSurfaceTemperature,
	MAX(snow_accumulation) AS MaxSnowAccumulation,
	AVG(external_temperature) AS AvgExternalTemperature,
	COUNT(*) AS ReadingCount,
	System.Timestamp AS EventTime,
	CASE
        WHEN AVG(ice_thickness) >= 30 AND AVG(surface_temperature) <= -2 THEN 'Safe'
        WHEN AVG(ice_thickness) >= 25 AND AVG(surface_temperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'
    END AS SafetyStatus
INTO
	[historical-data]
FROM
	[IoTHubCST8915]
GROUP BY
	IoTHub.ConnectionDeviceId, TumblingWindow(second, 300);



SELECT 
	IoTHub.ConnectionDeviceId AS location,
	AVG(ice_thickness) AS AvgIceThickness,
	MIN(ice_thickness) AS MinIceThickness,
	MAX(ice_thickness) AS MaxIceThickness,
	AVG(surface_temperature) AS AvgSurfaceTemperature,
	MIN(surface_temperature) AS MinSurfaceTemperature,
	MAX(surface_temperature) AS MaxSurfaceTemperature,
	MAX(snow_accumulation) AS MaxSnowAccumulation,
	AVG(external_temperature) AS AvgExternalTemperature,
	COUNT(*) AS ReadingCount,
	System.Timestamp AS timestamp,
	CASE
        WHEN AVG(ice_thickness) >= 30 AND AVG(surface_temperature) <= -2 THEN 'Safe'
        WHEN AVG(ice_thickness) >= 25 AND AVG(surface_temperature) <= 0 THEN 'Caution'
        ELSE 'Unsafe'

    END AS SafetyStatus
INTO
	[SensorAggregations]
FROM
	[IoTHubCST8915]
GROUP BY
	IoTHub.ConnectionDeviceId, TumblingWindow(second, 300);