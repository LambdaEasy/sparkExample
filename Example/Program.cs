using Microsoft.Spark.Sql;
using System;

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

            // Cargar datos en un DataFrame
            string filePath = args.FirstOrDefault(); // Reemplaza con la ruta de tu archivo CSV
            var df = spark.Read().Csv(filePath);

            // Mostrar el esquema del DataFrame
            df.PrintSchema();

            // Realizar una operación de transformación
            var result = df.Select("Column1", "Column2").Where("Column1 > 10");

            // Mostrar el resultado
            result.Show();

            // Detener la sesión de Spark
            spark.Stop();
        }
    }
}