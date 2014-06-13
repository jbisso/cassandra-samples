OBJS = simple.o bound.o SimpleClient.o BoundStatementsClient.o
CC = g++
DEBUG = -g
CFLAGS = -Wall -c $(DEBUG)
LFLAGS = -Wall $(DEBUG)

all : simple bound

simple : simple.o SimpleClient.o
	$(CC) $(LFLAGS) simple.o SimpleClient.o -o simple -luv -lssh2 -lcassandra

bound : bound.o BoundStatementsClient.o
	$(CC) $(LFLAGS) bound.o SimpleClient.o BoundStatementsClient.o -o bound -lcql -lboost_system -lboost_thread-mt

clean : 
	rm $(OBJS) simple bound
