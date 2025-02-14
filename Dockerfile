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
# docker run --platform=linux/amd64 -p 9000:8080 absaoss/eventgate:latest &
# 
# test via (provide payload):
# curl http://localhost:9000/2015-03-31/functions/function/invocations -d "{payload}"
#
# Deploy to AWS Lambda via ACR

FROM public.ecr.aws/lambda/python:3.11

# Directory with TRUSTED certs in PEM format
ARG TRUSTED_SSL_CERTS=./trusted_certs
# Artifacts for kerberized sasl_ssl
ARG SASL_SSL_ARTIFACTS=./sasl_ssl_artifacts

# Import trusted certs before doing anything else
RUN mkdir -p /opt/certs
COPY $TRUSTED_SSL_CERTS /opt/certs/
RUN for FILE in `ls /opt/certs/*.pem`; \
do \
    cat $FILE >> /etc/pki/tls/certs/ca-bundle.crt ;\
done

# Install
#  -  basics
RUN yum install -y wget tar xz bzip2-devel zlib-devel 
#  -  GCC ("which" is required during some makefiles)
RUN yum install -y which make gcc gcc-c++
#  -  dependencies for kerberos SASL_SSL
RUN yum install -y libffi-devel cyrus-sasl-devel cyrus-sasl-gssapi openssl-devel krb5-workstation

# librdkafka 
#  -  fetch 
RUN mkdir -p /tmp/env-install-workdir/librdkafka
WORKDIR /tmp/env-install-workdir/librdkafka
RUN wget https://github.com/edenhill/librdkafka/archive/v2.4.0.tar.gz
#  -  unpack
RUN tar -xf v2.4.0.tar.gz
#  -  build
WORKDIR /tmp/env-install-workdir/librdkafka/librdkafka-2.4.0
RUN ./configure && make && make install
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

# confluent-kafka
#  -  fetch
RUN mkdir -p /tmp/env-install-workdir/confluent-kafka
WORKDIR /tmp/env-install-workdir/confluent-kafka
RUN wget https://github.com/confluentinc/confluent-kafka-python/archive/v2.4.0.tar.gz
#  -  unpack
RUN tar -xf v2.4.0.tar.gz
#  -  build
WORKDIR /tmp/env-install-workdir/confluent-kafka/confluent-kafka-python-2.4.0
RUN CPPFLAGS="-I/usr/local/include" LDFLAGS="-L/opt" python setup.py install

# Cleanup
WORKDIR /root
RUN rm -rf /tmp/env-install-workdir

# PIP install
RUN pip install requests==2.31.0 urllib3==1.26.18 cryptography jsonschema PyJWT

# Lambda and SASL_SSL_Artifacts
RUN mkdir -p /opt/sasl_ssl_artifacts
COPY $SASL_SSL_ARTIFACTS /opt/sasl_ssl_artifacts/
COPY src/event_gate_lambda.py $LAMBDA_TASK_ROOT
COPY conf $LAMBDA_TASK_ROOT/conf

# Kerberos default CCACHE override due to KEYRING issues
ENV KRB5CCNAME=FILE:/tmp/krb5cc

# restore AWS Lambda working directory
WORKDIR /var/task

# Set lambda entry point as CMD
CMD ["event_gate_lambda.lambda_handler"]