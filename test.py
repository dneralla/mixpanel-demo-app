import pyspark
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

spark.conf.set("google.cloud.auth.service.account.enable", "true")
spark.conf.set("fs.gs.auth.service.account.email", "export-upload@mixpanel-prod-1.iam.gserviceaccount.com")
spark.conf.set("fs.gs.project.id", "mixpanel-prod-1")
spark.conf.set("fs.gs.auth.service.account.private.key", "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDKO8pWbPELODnx\nDZyTHztGPz7ILyH9rYZaEJhq2SSVVJoRl/FzV1+5YGrrKzmRbGeHJDEM2WQdYcMV\n+Pk4+uQGRv0IdHDzMIETOX0v3ZGaFGVxa4YFYJBY9iXcW3QrMU+w2pMiXOPIUpYK\nUu7T9LI6htRVIcUHbANlEKMZq0fMyuirxOBKwiEg4SIBBQOmbD5qZLBO0Kj8eoQX\nLijv4R+2438yrZiEC9ylPp6Mq/XOc6F31TKYeauuIFoWG52XlQOd0+7aTxpYHdXv\nqrMIgbDedWqL//hiZyOeMrhh+3MZI0SbPpQOJJt5WxnbWeDbv7VNECeeToPadD+R\naDz7KIV/AgMBAAECggEAAhMblwBYYu+jm7rlxwbXeaCw8zrdCFNE1ZKUF07CUur5\nqu1/UCHXktVaED5Ey8ykau2Jj3DTKZ8fIEM4KgV6zOPD8ZAFXHQx+GNl88Ynj5em\nnQEEGYRDH8pZ2wA3vVmx6dVE1r4L7e1eKh2Wuhz2R8A5tHPz6iJ1DiNoHWpWpmLZ\nMbT09yH2u8mIwA6PSBAQ6b3geoT1Vr26FveG7fQQk928eQINnm8MNBCIeDYeizpl\nrCY6Jh4p+2RyZ36hQ0i3pQp+9QhlMjMN2uGI/v40Rbh31KGeUpmh1mhSbIcpLcdm\nSt/IdAa3OI9nEbJZceAVBusCk+3Ogut4WQsRrQXRwQKBgQDncnCB0+SfbdDo1Tva\n6+ua/08VSbXDjluVgY/vnR+uIgJv1kQr9qMjaN7GxvjGncSWuVuOPx9k4fA9XRxJ\n5uwiCsiuPgIiu2edemWRhuQDFrnKSjHHuFyHBZzBaXnMlQufE9rNa8EyJTiFBMaV\naFf7Wu4FccLLBx8sEk7D2HewCwKBgQDfr/syBF8ccr+J9XYRXwAVgnPM8rUWGbtu\nr5CloS5yVsUmYXs/xWM2ClHPUzbD8HpOEcuvAEkO4vZXx5c5nf6FVke//3ydERod\nODad9FWkfNHhslPT6qMuEOESPtFbxU3HwUTHQVlm7HrwiSZi0huf4sQ6tCL6tjAC\nTatDXbwk3QKBgDXuJacq38ACj8ZhQYQ+qvS//wZ/2XnK5y5MNWTpIOOrixPSQqpX\n2yW6HwcEgB2Pz6orYNkhfkg8mYVC9/+Ebot0JdTsIAVF09wPFDG309OgbQRlC3p1\nTCIuPZuX30i7hiy4LKjnLepoX+Ym+bsAyFeKlaHxIym57BqR3YanHySHAoGAa5Q4\nbDq7/3OUeOYrjXkyEiY2SUglHWbsxPP8zhuxud3PXYEehHILH+9gnyR3P08Fk0xt\nuFGb0WQIc/kS2uwIf9oaEdXu+Z9C/vCNG/Rpx/0b978LH+1F457MkDjT+Z3vpXVm\n/amJNL3g9pQPeRFxpbirBUyjUg6TEliGWJTZKqUCgYEA3M3u31gc5L4hzxRZmiuE\ngBmowaxgVoZqrAofRF/ujkCI4budyyLqC7S1xfi4Y8kan71OKuUwEHqvW/GTXG4J\n5ly7f52GNzGvZzBssIId95VmyLu3pw3j4W8N/+W+OKwQN/LmcfsbgwXA7RM1c6dD\n6O144JiM95KpUXR7VQJNstA=\n-----END PRIVATE KEY-----\n")
spark.conf.set("fs.gs.auth.service.account.private.key.id", "0777fb41db82d0de8f4c634ab8d56bffcf07436a")

df = spark.read.table("samples.nyctaxi.trips")
df.write.format("json").mode("overwrite").save("gs://mixpanel-prod-1-warehouse-imports/testdatabricks/10456")
