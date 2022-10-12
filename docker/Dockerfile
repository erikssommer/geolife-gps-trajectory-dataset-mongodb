FROM mongo:6.0.1

COPY /docker/init-mongo.sh /docker-entrypoint-initdb.d/init-mongo.sh

# For windows machines
RUN sed -i -e 's/\r$//' /docker-entrypoint-initdb.d/init-mongo.sh