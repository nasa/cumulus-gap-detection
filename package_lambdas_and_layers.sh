# Package Lambda functions and layers
cp src/shared/gap_schema.sql src/gapCreateTable/
LAMBDAS_DIR="src"
ROOT_DIR="${PWD}"
rm -rf artifacts
mkdir -p artifacts/functions
mkdir -p artifacts/layers
docker build -f Dockerfile -t lambda-packager .
for lambda_path in $LAMBDAS_DIR/*/ ; do
    module_name=$(basename $lambda_path)
    echo -e "\n================================= Packaging $module_name\t================================= "
    docker run --rm \
      -v $(pwd)/$lambda_path:/app/lambda \
      -v $(pwd)/artifacts/:/artifacts/ \
      lambda-packager $module_name /app/lambda
done
rm src/gapCreateTable/gap_schema.sql

if [ ! -d "artifacts" ]; then
  echo "Source directory does not exist: artifacts"
  exit 1
fi

cp -r "artifacts" "gap_detection_module"

echo "Copied artifacts to gap_detection_module"
