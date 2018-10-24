
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
studentsPRdf = spark.read.format("csv").option("header", "false").load("/user/ken/studentsPR.csv")

studentsPRdf = spark.read.format("csv").option("header", "false").load("/user/ken/studentsPR.csv")
maleStudents = studentsPRdf.join(escuelasPRdf, 'idescuela').filter(studentsPRdf.sexo == 'M').filter((escuelasPRdf.ciudad == 'Ponce') | (escuelasPRdf.ciudad == 'San Juan')).filter(escuelasPRdf.nivel == 'Superior').select(studentsPRdf.idestudiante)
maleStudents.show()
