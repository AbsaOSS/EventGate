pip3 install -r src/requirements.txt -t dependencies/ --platform manylinux2014_x86_64 --python-version 3.12 --only-binary=:all: 
zip -r lambda_function.zip conf
cd src
zip -r ../lambda_function.zip .
cd ../dependencies
zip -r ../lambda_function.zip .
