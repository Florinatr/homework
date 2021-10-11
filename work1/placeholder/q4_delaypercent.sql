SELECT CompanyName,
    printf("%.2f",COUNT(CASE WHEN ShippedDate>RequiredDate THEN true ELSE NULL END)/ROUND(COUNT('Order'.Id))*100) as DelayPercent
FROM 'Order' LEFT OUTER JOIN Shipper
ON 'Order'.ShipVia = Shipper.Id
GROUP BY 'Order'.ShipVia ORDER BY DelayPercent desc;