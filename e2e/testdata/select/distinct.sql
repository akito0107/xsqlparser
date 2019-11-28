SELECT Count(*) AS DistinctCountries
FROM (SELECT DISTINCT Country FROM Customers);
