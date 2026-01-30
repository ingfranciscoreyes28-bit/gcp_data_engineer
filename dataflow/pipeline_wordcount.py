# Importa Apache Beam, el framework base para Dataflow
import apache_beam as beam

# Importa las opciones de configuración del pipeline
from apache_beam.options.pipeline_options import PipelineOptions


def run():

    # ===============================
    # CONFIGURACIÓN DEL PIPELINE
    # ===============================
    options = PipelineOptions(
        # Runner local (no cobra)
        # Para GCP real debes usar: DataflowRunner
        runner="DataflowRunner",

        # ID del proyecto en GCP
        project="gsp-data-engineer-04",

        # Región donde se ejecutaría Dataflow
        region="us-central1",

        # Bucket temporal que usa Dataflow
        # OJO: este bucket es el que genera el aviso de Soft Delete
        temp_location="gs://gsp-bucket-engineer-o5b/temp"
    )

    # ===============================
    # DEFINICIÓN DEL PIPELINE
    # ===============================

    # Crea el pipeline con las opciones anteriores
    with beam.Pipeline(options=options) as p:

        (
            p
            # Paso 1: leer archivo de texto desde Cloud Storage
            | "Leer archivo" >> beam.io.ReadFromText(
                "gs://dataflow-samples/shakespeare/kinglear.txt"
            )

            # Paso 2: separar cada línea en palabras
            # FlatMap transforma 1 línea → muchas palabras
            | "Separar palabras" >> beam.FlatMap(
                lambda line: line.split()
            )

            # Paso 3: contar cuántas veces aparece cada palabra
            | "Contar palabras" >> beam.combiners.Count.PerElement()

            # Paso 4: guardar el resultado en Cloud Storage
            | "Guardar resultados" >> beam.io.WriteToText(
                "gs://gsp-bucket-engineer-o5b/output/wordcount"
            )
        )
        print("Pipeline ejecutado correctamente")


# Punto de entrada del script
if __name__ == "__main__":
    run()
