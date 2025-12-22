rfq_fx = spark.read.csv(paths['rfq_input'], header=True, inferSchema=True)
rfq_fx.display()
