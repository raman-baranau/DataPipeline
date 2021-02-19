counter=0
while read -r line; do
  counter=$((counter+1))
  echo "Records processed: $counter"
  sleep 1
  gcloud pubsub topics publish projects/planningmeme/topics/data-without-user-location --message "$line"
done < "test-dataset-event-without-user-location.json"
