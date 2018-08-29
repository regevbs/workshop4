all: client server indexer httpServer
client: client.c 
	gcc client.c -libverbs -DEX4 -o client 
server: server.c
	gcc server.c -libverbs -DEX4 -o server 
indexer: indexer.c
	gcc indexer.c -libverbs -DEX4 -o indexer
httpServer:  nweb23.c
	gcc -O2 nweb23.c -libverbs -DEX4 -o nweb
clean:
	rm -f client
	rm -f server
	rm -f indexer