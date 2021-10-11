SELECT CategoryName, NumberOfProducts, AveragePrice, MinimumPrice, MaximumPrice, TotalUnitsOnOrder
FROM (SELECT CategoryId,
        COUNT(*) AS NumberOfProducts,
        printf("%.2f",AVG(UnitPrice)) AS AveragePrice, MIN(UnitPrice) AS MinimumPrice, MAX(UnitPrice) AS MaximumPrice, SUM(UnitsOnOrder) AS TotalUnitsOnOrder
     FROM Product
     GROUP BY CategoryId)
     LEFT OUTER JOIN Category
     ON CategoryId = Category.Id
WHERE NumberOfProducts>10
ORDER BY CategoryId;