# build via (docker root = project root):
# docker build -t absaoss/eventgate:latest .
#
# build [with customizations] via (docker root = project root):
# docker build -t absaoss/eventgate:latest \
# --build-arg TRUSTED_SSL_CERTS=./myTrustedCertsStorage \
# --build-arg SASL_SSL_ARTIFACTS=./mySaslSslCredentials \
# .
#
# run locally via:
# docker run --platform=linux/arm64 -p 9000:8080 absaoss/eventgate:latest &
# 
# test via (provide payload):
# curl http://localhost:9000/2015-03-31/functions/function/invocations -d "{payload}"
#
# Deploy to AWS Lambda via ECR

FROM --platform=linux/arm64 public.ecr.aws/lambda/python:3.13-arm64

# Directory with TRUSTED certs in PEM format
ARG TRUSTED_SSL_CERTS=./trusted_certs
# Artifacts for kerberized sasl_ssl
ARG SASL_SSL_ARTIFACTS=./sasl_ssl_artifacts

# Trusted certs
COPY $TRUSTED_SSL_CERTS /opt/certs/

RUN \
  echo "######################################################" && \
  echo "### Import trusted certs before doing anything else ###" && \
  echo "######################################################" && \
  for FILE in `ls /opt/certs/*.pem`; \
    do cat $FILE >> /etc/pki/tls/certs/ca-bundle.crt ; done && \
  echo "###############################################" && \
  echo "### Install                                  ###" && \
  echo "### -> Basics                                 ###" && \
  echo "### -> GCC (some makefiles require cmd which)###" && \
  echo "### -> dependencies for kerberos SASL_SSL   ###" && \
  echo "##############################################" && \
    dnf install -y \
      wget tar xz bzip2-devel zlib-devel \
      which make gcc gcc-c++ \
      libffi-devel cyrus-sasl-devel cyrus-sasl-gssapi openssl-devel krb5-workstation && \
  echo "#################" && \
  echo "### librdkafka ###" && \
  echo "#################" && \
    mkdir -p /tmp/env-install-workdir/librdkafka && \
    cd /tmp/env-install-workdir/librdkafka && \
    wget https://github.com/edenhill/librdkafka/archive/v2.4.0.tar.gz && \
    tar -xf v2.4.0.tar.gz && \
    cd /tmp/env-install-workdir/librdkafka/librdkafka-2.4.0 && \
    ./configure && make && make install && \
  echo "###################" && \
  echo "### pip installs ###" && \
  echo "###################" && \
    pip install requests==2.31.0 urllib3==1.26.18 setuptools cryptography jsonschema PyJWT psycopg2-binary && \
  echo "######################" && \
  echo "### confluent-kafka ###" && \
  echo "######################" && \
    mkdir -p /tmp/env-install-workdir/confluent-kafka && \
    cd /tmp/env-install-workdir/confluent-kafka && \
    wget https://github.com/confluentinc/confluent-kafka-python/archive/v2.4.0.tar.gz && \
    tar -xf v2.4.0.tar.gz && \
    cd /tmp/env-install-workdir/confluent-kafka/confluent-kafka-python-2.4.0 && \
    CPPFLAGS="-I/usr/local/include" LDFLAGS="-L/opt" python setup.py install && \
  echo "##############" && \
  echo "### cleanup ###" && \
  echo "##############" && \
    cd /root && \
    rm -rf /tmp/env-install-workdir
  
# Lambda and SASL_SSL_Artifacts
COPY $SASL_SSL_ARTIFACTS /opt/sasl_ssl_artifacts/
COPY src/ $LAMBDA_TASK_ROOT/
COPY conf $LAMBDA_TASK_ROOT/conf

# Mark librdkafka to LD_LIBRARY_PATH  
# Kerberos default CCACHE override due to KEYRING issues
ENV \
  LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib \
  KRB5CCNAME=FILE:/tmp/krb5cc

# Set lambda entry point as CMD
CMD ["event_gate_lambda.lambda_handler"]
