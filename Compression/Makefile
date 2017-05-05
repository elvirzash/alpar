# Make file for MPI BZIP2
SHELL=/bin/sh

# Compiler to use
# You may need to change CC to something like CC=mpiCC
CC=c++

# Where you want mpibzip2 installed when you do 'make install'
PREFIX=/usr

all: mpibzip2

# Standard mpibzip2 compile
mpibzip2: mpibzip2.cpp
	$(CC) -O2 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -o mpibzip2 mpibzip2.cpp -lbz2 -lmpi -lpthread

# Choose this if you want to compile in a static version of the libbz2 library
mpibzip2-static: libbz2.a mpibzip2.cpp
	$(CC) -O2 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -o mpibzip2 mpibzip2.cpp -lmpi -lpthread -I. -L. -lbz2

# Compatability mode for 32bit file sizes (less than 2GB) and systems
# that have compilers that treat int as 64bit natively
mpibzip2-compat: mpibzip2.cpp
	$(CC) -O2 -o mpibzip2 mpibzip2.cpp -lbz2 -lmpi -lpthread

# Compile for Sun/Solaris MPI
sun: mpibzip2.cpp
	mpCC -O3 -D_LARGEFILE64_SOURCE -D_FILE_OFFSET_BITS=64 -o mpibzip2 mpibzip2.cpp -pthread -lpthread -lmpi_mt -lbz2

# Install the binary pbzip2 program and man page
install: mpibzip2
	if ( test ! -d $(PREFIX)/bin ) ; then mkdir -p $(PREFIX)/bin ; fi
	if ( test ! -d $(PREFIX)/man ) ; then mkdir -p $(PREFIX)/man ; fi
	if ( test ! -d $(PREFIX)/man/man1 ) ; then mkdir -p $(PREFIX)/man/man1 ; fi
	cp -f mpibzip2 $(PREFIX)/bin/mpibzip2
	chmod a+x $(PREFIX)/bin/mpibzip2
	cp -f mpibzip2.1 $(PREFIX)/man/man1
	chmod a+r $(PREFIX)/man/man1/mpibzip2.1

clean:
	rm mpibzip2
