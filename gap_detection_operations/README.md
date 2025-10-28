Collections of around 1 million granules or more will need to be triggered from EC2, as they will surpass the API gateway timeout limit if ran from gapConfig API.

There are two folders here, each with different uses:

In the invoke_single_collection directory, the invoke_gap_config.sh script will run for a single collection. In the bulk_invoke directory, the lambda_bulk_invoker.py script will run for a list of collections.


To run for a single collection:

- Launch or use an existing EC2 instance in the same VPC as gapConfig API.
- Prepare the input file and script provided in the invoke_single_collection folder. The event.json file needs to be modified to run for your specified collection.
Usage: './invoke_gap_config.sh'
Check the response: 'cat response.json'


lambda_bulk_invoker.py is used for larger lists of collections and will process them sequentially.

To run a list of collections:

- Create lambda_bulk_invoker.py on the EC2 instance. Paste the code from this repository into that file.
- The EC2 Instance should have sqs:GetQueueUrl and sqs:GetQueueAttributes permissions for the gapDetectionIngestQueue
- The lambda name for gapConfig, the csv file, and the queue name are specified as command line arguments.
Usage: python3 lambda_queue_batch_processor.py <lambda_function_name> <csv_file> <queue_name>
Example: python3 lambda_queue_batch_processor.py gesdisc-cumulus-uat-gapConfig collections.csv gesdisc-cumulus-uat-gapDetectionIngestQueue