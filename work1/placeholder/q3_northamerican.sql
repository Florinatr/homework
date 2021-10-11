 SELECT Id,  ShipCountry, (CASE WHEN ShipCountry IN ('USA','Mexico','Canada') THEN 'NorthAmerica' ELSE 'OtherPlcae' END) as ShipRegion
FROM 'Order'
WHERE Id >= 15445  ORDER BY ID
LIMIT 20;