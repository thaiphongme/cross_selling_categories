WITH FINAL_table AS (
	WITH ranking2 AS (
		WITH adj AS (
			WITH top2categories AS (			
				WITH transactions AS (
					SELECT 
					TRANSACTION_ID,
					SKU_Category,
					sum(Quantity) AS quantity
					
					FROM phong_db.scanner_data
					
					WHERE TRANSACTION_ID IN (
					
						SELECT DISTINCT 
						Transaction_ID
						
						FROM  phong_db.scanner_data
						
						GROUP BY 1 
						
						HAVING count(DISTINCT SKU_Category)>1
						
						
			
					)
					
					GROUP BY 1,2 ORDER BY 3 DESC 
					
				)
				
				SELECT 
				t.*,
				ROW_NUMBER () OVER (PARTITION BY TRANSACTION_ID ORDER BY quantity DESC) AS ranking,
				sum(quantity) OVER (PARTITION BY TRANSACTION_ID) AS total_quantity_per_transation
				FROM transactions t
				
				ORDER BY  total_quantity_per_transation DESC, ranking ASC
				
			)
			
			
			SELECT 
			t.*,
			sum(quantity) OVER (PARTITION BY TRANSACTION_ID) AS total_quantity_per_transation_adj
			
			FROM top2categories t
			
			WHERE ranking <=2
			
			ORDER BY  total_quantity_per_transation DESC, ranking ASC
			
		)
		
		SELECT 
		a.*,
		dense_rank() OVER(ORDER BY total_quantity_per_transation_adj DESC ) AS ranking2
		
		FROM adj a
		
	)
	
	SELECT 
	r.*,
	LAG(SKU_Category) over() AS lag_category
	FROM ranking2 r
	
	WHERE ranking2 <=3
	
)

SELECT 
ranking2 AS ranking,
concat(SKU_category," + ",lag_category) AS cross_selling,
total_quantity_per_transation_adj AS total_quantity

FROM FINAL_table

WHERE ranking=2