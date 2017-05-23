.PHONY = clean all

CXX = g++ -std=c++11
FLAGS = -Wextra -Wall -lpthread
FILES_TO_CREATE =  Search tar
FILES_TO_DELETE = *.o  MapReduceFramework.a Search
FILES_FOR_TAR = *.cpp *.h Makefile README
FILES_FOR_SEARCH = Search.cpp  MapReduceFramework.h
GIVEN_HEADERS = MapReduceClient.h MapReduceFramework.h
FILES_FOR_SEARCH_FRAME = Search.*
FILES_FOR_FRAME = MapReduceFramework.cpp

Search: MapReduceFramework.a Search.o
	$(CXX) -lpthread Search.o -L. MapReduceFramework.a -o Search

MapReduceFramework.a:  MapReduceFramework.o
	ar rcs MapReduceFramework.a  MapReduceFramework.o

MapReduceFramework.o: $(GIVEN_HEADERS)  $(FILES_FOR_FRAME)
	$(CXX) -c $(FLAGS) MapReduceFramework.cpp

Search.o: $(FILES_FOR_SEARCH)
	$(CXX) -c $(FLAGS) Search.cpp

tar: $(FILES_FOR_TAR)
	tar cvf ex3.tar $(FILES_FOR_TAR)

all: $(FILES_TO_CREATE)

clean:
	 rm $(FILES_TO_DELETE)