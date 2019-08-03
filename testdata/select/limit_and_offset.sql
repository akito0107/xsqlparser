SELECT product, SUM(quantity) AS product_units FROM orders
	WHERE region IN (SELECT region FROM top_regions)
	ORDER BY product_units LIMIT 100 OFFSET 20;