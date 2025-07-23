Collections of around 1 million granules or more will need to be triggered from EC2, as they will surpass the API gateway timeout limit if ran from gapConfig API. 

There are two functions here, each with different uses: 

invoke_gap_config.sh is used for single collections that are greater than ~1 million granules 

To run: 

1. Launch or use an existing EC2 instance in the same VPC as gapConfig API. 
2. Prepare the input file and script provided in this folder. The event.json file needs to be modified to run for your specified collection. 
3. Run the script: './invoke_gap_config.sh'
4. Check the response: 'cat response.json'

lambda_bulk_invoker.py is used for larger lists of collections and will process them sequentially. 

To run: 
1. Create a collections.csv with first column collection ID and second column version. Third column for tolerance is optional
2. The lambda name for gapConfig and the csv file are specified as command line arguments. 
2. EXAMPLE RUN: python3 lambda_bulk_invoker.py gapConfigLambdaName collections.csv