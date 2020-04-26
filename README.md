# eatinghouse
To testing
docker build . -t testing
docker run -e AZURE_CLIENT_ID=0d8724f5-8e40-4181-8049-9d905e0f347b -e AZURE_CLIENT_SECRET=8597ff13-53ac-4d52-ab8a-cf11168fe6eb -e AZURE_TENANT_ID=8d5a714c-81e5-411c-a53e-b199eb51f860 -e KEY_VAULT_NAME=thachstockservers -p 6789:6789 -rm testing

# Docker swarm
docker swarm init --advertise-addr 10.1.3.4:2377 --listen-addr 10.1.3.4:2377
docker network create -d overlay overlay-network

# manager
docker swarm join --token SWMTKN-1-4asfnwq5kmfagx9xixzzcc28hekf3fqotne6cloqokuten6t1k-8qndkuaaijalxmgrvwumxnl9g 10.1.3.4:2377 \
 --advertise-addr 10.1.3.7:2377 --listen-addr 10.1.3.7:2377
# Worker
docker swarm join --token SWMTKN-1-4asfnwq5kmfagx9xixzzcc28hekf3fqotne6cloqokuten6t1k-bvsiega1r045tnoeo92vve8n8 10.1.3.4:2377 \
 --advertise-addr 10.1.3.9:2377 --listen-addr 10.1.3.9:2377

docker service create --name matchtest --network matching-network -p 80:8890 matching:latest --replicas 10 \
-e AZURE_CLIENT_ID=0d8724f5-8e40-4181-8049-9d905e0f347b \
-e AZURE_CLIENT_SECRET=8597ff13-53ac-4d52-ab8a-cf11168fe6eb \
-e AZURE_TENANT_ID=8d5a714c-81e5-411c-a53e-b199eb51f860 \
-e KEY_VAULT_NAME=thachstockservers -p 6789:6789 \
--with-registry-auth thachrocky/eatinghouse-api:latest

# ddl on the eatinghouse database

# setup authentication
