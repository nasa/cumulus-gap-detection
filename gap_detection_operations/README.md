This function is used to run gapConfig on collections of ~1 million granules or more from EC2. 

Collections of around 1 million granules or more will need to be triggered from EC2, as they will surpass the API gateway timeout limit if ran from gapConfig API. 

To run: 

1. Launch or use an existing EC2 instance in the same VPC as gapConfig API. 
2. Prepare the input file and script provided in this folder. The event.json file needs to be modified to run for your specified collection. 
3. Run the script: './invoke_gap_config.sh'
4. Check the response: 'cat response.json'