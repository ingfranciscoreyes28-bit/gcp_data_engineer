python wordcount.py --runner DataflowRunner --project gcp-data-engineer-07- --region us-central1 --staging_location gs://gcp-bucket-engineer-07/staging/ --temp_location gs://gcp-bucket-engineer-07/temp/ --template_location gs://gcp-bucket-engineer-07/templates/wordcount_template

python3 wordcount.py \
    --runner DataflowRunner \
    --project gcp-data-engineer-07 \
    --region us-central1 \
    --staging_location gs://gcp-bucket-engineer-07/staging/ \
    --temp_location gs://gcp-bucket-engineer-07/temp/ \
    --template_location gs://gcp-bucket-engineer-07/templates/wordcount_template




pip install apache-beam==2.47.0
pip install protobuf==4.24.3