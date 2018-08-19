all: client server indexer
client: client.c 
	gcc client.c -libverbs -DEX4 -o client 
server: server.c
	gcc server.c -libverbs -DEX4 -o server 
indexer: indexer.clean
    gcc indexer.c -libverbs -DEX4 -o indexer 
clean:
	rm -f client
	rm -f server
    rm -f indexer
