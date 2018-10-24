spark = SparkSession.builder.getOrCreate()

escuelasPRSchema = StructType([
	StructField('region', StringType()), 
	StructField('distrito', StringType()), 
	StructField('ciudad', StringType()), 
	StructField('idescuela', IntegerType()), 
	StructField('nombreescuela', StringType()), 
	StructField('nivel', StringType()), 
	StructField('serie', IntegerType())])

studentsPRSchema = StructType([
	StructField('region', StringType()), 
	StructField('distrito', StringType()), 
	StructField('idescuela', IntegerType()), 
	StructField('nombreescuela', StringType()),
	StructField('nivel', StringType()), 
	StructField('sexo', StringType()), 
	StructField('idestudiante', IntegerType())])

escuelasPRdf = spark.read.format("csv").option("header", "false").load("/user/ken/escuelasPR.csv")

schoolperdistrict = escuelasPRdf.filter(escuelasPRdf.region == 'Arecibo').groupBy('distrito', 'ciudad').count()
schoolperdistrict.show()