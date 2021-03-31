gcloud dataflow jobs run DESCOMPRIMIR \
    --region=us-central1 \
    --gcs-location gs://dataflow-templates/latest/Bulk_Decompress_GCS_Files \
    --parameters \
inputFilePattern=gs://billboard8817/charts.csv.zip,\
outputDirectory=gs://billboard8817/descomprimido,\
outputFailureFile=gs://billboard8817/failure 


