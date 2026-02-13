python wordcount.py --runner DataflowRunner --project gcp-data-engineer-06- --region us-central1 --staging_location gs://gcs-bucket-engineer-06/staging/ --temp_location gs://gcs-bucket-engineer-06/temp/ --template_location gs://gcs-bucket-engineer-06/templates/wordcount_template

python3 wordcount.py \
    --runner DataflowRunner \
    --project gcp-data-engineer-06 \
    --region us-central1 \
    --staging_location gs://gcs-bucket-engineer-06/staging/ \
    --temp_location gs://gcs-bucket-engineer-06/temp/ \
    --template_location gs://gcs-bucket-engineer-06/templates/wordcount_template




pip install apache-beam==2.47.0
pip install protobuf==4.24.3