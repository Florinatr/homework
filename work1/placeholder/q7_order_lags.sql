SELECT Id, OrderDate, LAG(OrderDate,1) OVER(ORDER BY OrderDate) AS PreviousOrderDate,
    printf("%.2f",julianday(OrderDate)-julianday(LAG(OrderDate,1) OVER(ORDER BY OrderDate))) AS DateGap
FROM 'Order'
WHERE CustomerID = 'BLONP'
ORDER BY OrderDate
LIMIT 10;