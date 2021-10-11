SELECT Y.RegionDescription, Y.FirstName,  Y.LastName, Y.BirthDate
FROM (SELECT E.FirstName, E.LastName, E.BirthDate, R.Id, R.RegionDescription
	FROM Employee AS E
	LEFT OUTER JOIN EmployeeTerritory AS ET
	ON E.Id = ET.EmployeeId
	LEFT OUTER JOIN Territory AS T
	ON ET.TerritoryId = T.Id 
	LEFT OUTER JOIN Region AS R
	ON T.RegionId = R.Id
	ORDER BY e.BirthDate DESC) AS Y
GROUP BY Y.id
ORDER BY Y.id;