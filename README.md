En este proceso se leen los requerimientos de un archivo .json con Spark a través del objeto GestionReq.
Posteriormente se ejecuta el proceso de lectura del input.
Después se aplican las transformaciones y se guarda la información en el output.
Esta aplicación se ekecuta en un contenedor de Docker, donde estará la salida final del proceso.
Utiliza el cluster de Spark, levantado con Docker.

spark-submit --master local[*] --class SPARKDIN.Proceso target/ETL-DIN-PRUEBA-1.0-SNAPSHOT-shaded.jar
