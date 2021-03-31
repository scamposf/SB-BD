gcloud dataproc clusters create billcluster \
 --scopes=default \
 --region "us-east4" --zone "us-east4-b" \
 --initialization-actions=gs://dataproc-initialization-actions/jupyter/jupyter.sh \
 --master-machine-type n1-standard-2 \
 --master-boot-disk-size 200 \
  --num-workers 3 \
--worker-machine-type n1-standard-2 \
--worker-boot-disk-size 200 \
--image-version 1.3