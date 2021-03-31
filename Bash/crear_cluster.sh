REGION="us-east1"

gcloud dataproc clusters create spark-dwh \
 --scopes=default \
 --region=${REGION} \
 --optional-components=ANACONDA \
 --metadata PIP_PACKAGES="google-cloud-storage" \
 --initialization-actions=gs://dataproc-initialization-actions/python/pip-install.sh \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 50 \
 --num-workers 3 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 50 \
--image-version 1.4
