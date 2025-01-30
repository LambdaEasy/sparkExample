using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System;
using System.Collections.Generic;
using System.Linq;

namespace SparkExample
{
    class Program
    {
        static void Main(string[] args)
        {
            // Crear una sesión de Spark
            var spark = SparkSession
                .Builder()
                .AppName("Simple Spark Example")
                .GetOrCreate();

            // Crear una lista de filas con datos (manualmente, sin archivo CSV)
            var data = new List<object[]>
                {
                    new object[] { 1, "Alice" },
                    new object[] { 2, "Bob" },
                    new object[] { 3, "Charlie" },
                    new object[] { 4, "David" },
                    new object[] { 5, "Eve" }
                };

            // Definir el esquema para los datos
            var schema = new StructType(new[]
            {
                    new StructField("ID", new IntegerType()),
                    new StructField("Name", new StringType())
                });

            // Crear un DataFrame a partir de los datos y el esquema
            var df = spark.CreateDataFrame(data.Select(row => new GenericRow(row)).ToList(), schema);

            // Mostrar el esquema del DataFrame
            df.PrintSchema();

            // Realizar una operación de transformación
            var result = df.Filter("ID > 2");

            // Mostrar el resultado
            result.Show();

            // Detener la sesión de Spark
            spark.Stop();
        }
    }
}
