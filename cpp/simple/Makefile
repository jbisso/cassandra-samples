OBJS = simple.o bound.o SimpleClient.o BoundStatementsClient.o

CC = g++
DRIVER_DIR = /Users/jimbisso/work/repos/datastax/cpp-driver

DEBUG = -g
CPPFLAGS = -Wall -c $(DEBUG) -I$(DRIVER_DIR)/include
LFLAGS = -Wall $(DEBUG)

LIBS = -L$(DRIVER_DIR) -luv -lssh2 -lcassandra

all: simple bound

simple: SimpleClient.o simple.o
	$(CC) $(LFLAGS) -o simple SimpleClient.o simple.o $(LIBS)

bound: bound.o BoundStatementsClient.o
	$(CC) $(LFLAGS) -o bound bound.o SimpleClient.o BoundStatementsClient.o $(LIBS)

clean:
	rm $(OBJS) simple bound

