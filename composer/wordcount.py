# Importa Apache Beam, el framework base para Dataflow
import apache_beam as beam
# Importa las opciones de configuración del pipeline
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    # CONFIGURACIÓN DEL PIPELINE
    options = PipelineOptions(
        # Runner local (no cobra)
        # Para GCP real debes usar: DataflowRunner  
        runner="DataflowRunner",

        # ID del proyecto en GCP
        project="gcp-data-engineer-06",

        # Región donde se ejecutaría Dataflow
        region="us-central1",

        # Bucket temporal que usa Dataflow
        # OJO: este bucket es el que genera el aviso de Soft Delete
        temp_location="gs://gcs-bucket-engineer-06/temp"
    )
    # DEFINICIÓN DEL PIPELINE
    # Crea el pipeline con las opciones anteriores
    with beam.Pipeline(options=options) as p:
        (
            p
            # Paso 1: leer archivo de texto desde Cloud Storage
            | "Leer archivo" >> beam.io.ReadFromText(
                "gs://dataflow-samples/shakespeare/kinglear.txt"
            )
            | "Separar palabras" >> beam.FlatMap(
                lambda line: line.split()
            )
            | "Contar palabras" >> beam.combiners.Count.PerElement()
            | "Guardar resultados" >> beam.io.WriteToText(
                "gs://gcs-bucket-engineer-06/output/wordcount"
            )
        )
        print("Pipeline ejecutado correctamente")

# Punto de entrada del script
if __name__ == "__main__":
    run()
