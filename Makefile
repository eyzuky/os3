.PHONY = clean all

CXX = g++ -std=c++11
FLAGS = -Wextra -Wall -lpthread
FILES_TO_CREATE =  test tar
FILES_TO_DELETE = *.o  MapReduceFramework.a test
FILES_FOR_TAR = *.cpp *.h Makefile README
FILES_FOR_SEARCH = test.cpp  MapReduceFramework.h
GIVEN_HEADERS = MapReduceClient.h MapReduceFramework.h
FILES_FOR_SEARCH_FRAME = test.*
FILES_FOR_FRAME = MapReduceFramework.cpp

test: MapReduceFramework.a test.o
	$(CXX) -lpthread test.o -L. MapReduceFramework.a -o test

MapReduceFramework.a:  MapReduceFramework.o
	ar rcs MapReduceFramework.a  MapReduceFramework.o

MapReduceFramework.o: $(GIVEN_HEADERS)  $(FILES_FOR_FRAME)
	$(CXX) -c $(FLAGS) MapReduceFramework.cpp

test.o: $(FILES_FOR_SEARCH)
	$(CXX) -c $(FLAGS) test.cpp

tar: $(FILES_FOR_TAR)
	tar cvf ex3.tar $(FILES_FOR_TAR)

all: $(FILES_TO_CREATE)

clean:
	 rm $(FILES_TO_DELETE)