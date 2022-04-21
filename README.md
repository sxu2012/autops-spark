# Spark Mini Project Automobile post-sales report
For this project, we are leveraging Spark as the new engine to redesign the Hadoop auto post-sales report project that was previously done in MapReduce.
## Read the input data CSV file
from pyspark import SparkContext  
sc = SparkContext('local', 'autops')  
raw_rdd = sc.textFile("data.csv")
raw_rdd.take(2)

['1,I,VXIO456XLBB630221,Nissan,Altima,2003,2002-05-08,Initial sales from TechMotors',
 '2,I,INU45KIOOPA343980,Mercedes,C300,2015,2014-01-01,Sold from EuroMotors']

 ## Perform map operation
 def extract_vin_key_value(line):  
    arr = line.split(',')
    return (arr[2], (arr[3], arr[5]))
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))
vin_kv.collect()

[('VXIO456XLBB630221', ('Nissan', '2003')),
 ('INU45KIOOPA343980', ('Mercedes', '2015')),
 ('VXIO456XLBB630221', ('', '')),
 ('VXIO456XLBB630221', ('', '')),
 ('VOME254OOXW344325', ('Mercedes', '2015')),
 ('VOME254OOXW344325', ('', '')),
 ('VXIO456XLBB630221', ('', '')),
 ('EXOA00341AB123456', ('Mercedes', '2016')),
 ('VOME254OOXW344325', ('', '')),
 ('VOME254OOXW344325', ('', '')),
 ('EXOA00341AB123456', ('', '')),
 ('EXOA00341AB123456', ('', '')),
 ('VOME254OOXW344325', ('', '')),
 ('UXIA769ABCC447906', ('Toyota', '2017')),
 ('UXIA769ABCC447906', ('', '')),
 ('INU45KIOOPA343980', ('', ''))]

 ## Perform group aggregation to populate make and year to all the records
 def populate_make(lines):   
    #first pass to find the make and year

    for ln in lines:
        if ln[0] != '':
            make = ln[0]
            year = ln[1]
            break
    #end pass to fill in make and year
    new_lines = [(make, year) for ln in lines]

    return new_lines

enhance_make = vin_kv.groupByKey().flatMap(lambda x: populate_make(list(x[1])))
enhance_make.collect()

[('Nissan', '2003'),
 ('Nissan', '2003'),
 ('Nissan', '2003'),
 ('Nissan', '2003'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2015'),
 ('Mercedes', '2016'),
 ('Mercedes', '2016'),
 ('Mercedes', '2016'),
 ('Toyota', '2017'),
 ('Toyota', '2017')]

 ## Count number of occurrence for accidents for the vehicle make and year:
 make_year_count = make_kv.reduceByKey(lambda a,b: a+b)
 make_year_count.collect()

 [('Nissan-2003', 4),
 ('Mercedes-2015', 7),
 ('Mercedes-2016', 3),
 ('Toyota-2017', 2)]