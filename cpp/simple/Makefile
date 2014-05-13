OBJS = simple.o bound.o SimpleClient.o BoundStatementsClient.o
CC = g++
CPPLIBDIR = $HOME/work/repos/jbisso/cpp-driver
DEBUG = -g
CFLAGS = -Wall -c $(DEBUG)
LFLAGS = -Wall $(DEBUG)

all : simple bound

simple : simple.o SimpleClient.o
	$(CC) $(LFLAGS) simple.o SimpleClient.o -o simple -llibuv -llibssh2 -llibcassandra.0.7.0.dylib

bound : bound.o BoundStatementsClient.o
	$(CC) $(LFLAGS) bound.o SimpleClient.o BoundStatementsClient.o -o bound -lcql -lboost_system -lboost_thread-mt

clean : 
	rm $(OBJS) simple bound
