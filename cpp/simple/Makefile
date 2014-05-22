OBJS = simple.o bound.o SimpleClient.o BoundStatementsClient.o
CC = g++
DEBUG = -g
CFLAGS = -Wall -c $(DEBUG) -std=c++11
LFLAGS = -Wall $(DEBUG)

all : demo

demo : demo.o
	$(CC) $(LFLAGS) demo.o -o demo -llibuv -llibssh2 -llibcassandra.0.7.0.dylib

clean : 
	rm $(OBJS) demo
