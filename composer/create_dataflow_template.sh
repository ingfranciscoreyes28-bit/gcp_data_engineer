python wordcount.py --runner DataflowRunner --project gcp-data-engineer-09 --region us-central1 --staging_location gs://gcp-bucket-777/staging/ --temp_location gs://gcp-bucket-777/temp/ --template_location gs://gcp-bucket-777/templates/wordcount_template


pip install apache-beam==2.47.0
pip install protobuf==4.24.3