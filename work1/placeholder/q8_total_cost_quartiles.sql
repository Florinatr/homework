SELECT CompanyName, CustomerId, TExpenditures
FROM (SELECT IFNULL(C.CompanyName, "MISSING_NAME") AS CompanyName, CI.CustomerId, CI.TExpenditures,
    	NTILE(4) OVER(ORDER BY CAST(CI.TExpenditures AS float)) AS Bucket
    FROM (SELECT O.CustomerId ,
    		printf("%.2f",SUM((OrDe.UnitPrice*OrDe.Quantity))) AS TExpenditures
    	FROM "Order" AS O
    		LEFT OUTER JOIN OrderDetail AS OrDe
    		ON O.Id = OrDe.OrderId
    	GROUP BY O.CustomerId) AS CI
    	LEFT OUTER JOIN Customer AS C 
    	ON CI.CustomerId = C.Id) AS F
WHERE Bucket=1
ORDER BY CAST(TExpenditures AS float);