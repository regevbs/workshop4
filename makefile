all: client server
client: client.c 
	gcc client.c -libverbs -DEX3 -o client 
server: server.c
	gcc server.c -libverbs -DEX3 -o server 
clean:
	rm -f client
	rm -f server
