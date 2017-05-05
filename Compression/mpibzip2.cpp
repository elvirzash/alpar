/*
 *	File  : mpibzip2.cpp
 *
 *	Title : MPI Parallel BZIP2 (mpibzip2)
 *
 *	Author: Jeff Gilchrist (http://gilchrist.ca/jeff/)
 *           - uses libbzip2 by Julian Seward (http://sources.redhat.com/bzip2/)
 *
 *	Date  : July 18, 2007
 *
 *
 *  Contributions
 *  -------------
 *  Bryan Stillwell <bryan@bokeoa.com> - code cleanup
 *  Dru Lemley [http://lemley.net/smp.html] - help with large file support
 *  Richard Russon <ntfs@flatcap.org> - help fix decompression bug
 *  Joergen Ramskov <joergen@ramskov.org> - initial version of man page
 *  Peter Cordes <peter@cordes.ca> - code cleanup
 *  Jindrich Novy <jnovy@redhat.com> - code cleanup and bug fixes
 *  Paul Pluzhnikov <paul@parasoft.com> - fixed minor memory leak
 *
 *
 * This program, "mpibzip2" is copyright (C) 2005-2007 Jeff Gilchrist.
 * All rights reserved.
 *
 * The library "libbzip2" which mpibzip2 uses, is copyright
 * (C) 1996-2002 Julian R Seward.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 
 * 2. The origin of this software must not be misrepresented; you must 
 *    not claim that you wrote the original software.  If you use this 
 *    software in a product, an acknowledgment in the product 
 *    documentation would be appreciated but is not required.
 * 
 * 3. Altered source versions must be plainly marked as such, and must
 *    not be misrepresented as being the original software.
 * 
 * 4. The name of the author may not be used to endorse or promote 
 *    products derived from this software without specific prior written 
 *    permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * 
 * Jeff Gilchrist, Ottawa, Canada.
 * mpibzip2@compression.ca
 * mpibzip2 version 0.6 of July 18, 2007
 * 
 */
#include <deque>
#include <vector>
#include <sys/stat.h>
#include <sys/time.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <mpi.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <utime.h>
#include <unistd.h>
#include <bzlib.h>

// uncomment for debug output
//#define MPIBZIP2_DEBUG

//#define OUTPUT_MODE		stdout
#define OUTPUT_MODE		stderr

#define	FILE_MODE	(S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

#ifndef O_BINARY
#define O_BINARY 0
#endif

#ifdef _LARGEFILE64_SOURCE
typedef off_t			uint_special;
#else
typedef unsigned int	uint_special;
#endif

typedef struct
{
	char *buf;
	unsigned int bufSize;
} outBuff;

typedef struct
{
	char **buf;
	unsigned int *bufSize;
	int *blockNum;
	long head, tail;
	int full, empty;
	pthread_mutex_t *mut;
	pthread_cond_t *notFull, *notEmpty;
} queue;

typedef struct
{
	uint_special dataStart;
	uint_special dataSize;
} bz2BlockListing;

//
// GLOBALS
//
static int allDone = 0;
static int QUEUESIZE = 2;
static int NumBlocks = 0;
static int BlockSizeBytes = 0;
static int Verbosity = 0;
static int QuietMode = 1;
static int OutputStdOut = 0;
static int BWTblockSize = 9;
static int FileListCount = 0;
static std::vector <outBuff> OutputBuffer;
static pthread_mutex_t *OutMutex = NULL;
static pthread_mutex_t *MemMutex = NULL;
static struct stat fileMetaData;
static char *sigInFilename = NULL;
static char *sigOutFilename = NULL;
static char BWTblockSizeChar = '9';
MPI_Datatype blockType;
int MyID = -1;
int TotalPS = -1;
int MasterID = -1;


void mySignalCatcher(int);
char *memstr(char *, int, char *, int);
int producer_decompress(int, uint_special, queue *);
void consumer_decompress();
void *fileWriter(void *);
int producer(int, int, uint_special, int, queue *);
void consumer();
queue *queueInit(int);
void queueDelete(queue *);
void queueAdd(queue *, char *, unsigned int, int);
char *queueDel(queue *, unsigned int *, int *);
int getFileMetaData(char *);
int writeFileMetaData(char *);
int testBZ2ErrorHandling(int, BZFILE *, int);
int testCompressedData(char *);
void MasterSlaveModelInitialize();
void kill_slaves();


/*
 *********************************************************
 */
void mySignalCatcher(int n)
{
	struct stat statBuf;
	int ret = 0;
	
	allDone = 1;

	fprintf(OUTPUT_MODE, "\n *Control-C or similar caught in rank %d, quitting...\n", MyID);
	#ifdef MPIBZIP2_DEBUG
	fprintf(OUTPUT_MODE, " Infile: %s   Outfile: %s\n", sigInFilename, sigOutFilename);
	#endif

	// only cleanup files if we did something with them	
	if ((sigInFilename == NULL) || (sigOutFilename == NULL))
	{
		if (MyID == MasterID) 
			kill_slaves();
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		exit(1);
	}

	// Only cleanup files if we are Master
	if (MyID == MasterID) 
	{
		// check to see if input file still exists	
		ret = stat(sigInFilename, &statBuf);
	    if (ret == 0)
	    {
		    // only want to remove output file if input still exists
			if (QuietMode != 1)
	            fprintf(OUTPUT_MODE, "Deleting output file: %s, if it exists...\n", sigOutFilename);
			ret = remove(sigOutFilename);
	        if (ret != 0)
	            fprintf(OUTPUT_MODE, " *WARNING: Deletion of output file (apparently) failed.\n");
	    }
	    else
	    {
			fprintf(OUTPUT_MODE, " *WARNING: Output file was not deleted since input file no longer exists.\n");
			fprintf(OUTPUT_MODE, " *WARNING: Output file: %s, may be incomplete!\n", sigOutFilename);
	    }
	}

	if (MyID == MasterID) 
		kill_slaves();
	// wait for all slave MPI ranks to get here before exiting
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	exit(1);
}

/*
 *********************************************************
   Initialize MPI model
*/
void MasterSlaveModelInitialize()
{
	MasterID = 0;
	if (MyID == MasterID) 
	{ 
		// do something only if Master
		#ifdef MPIBZIP2_DEBUG
	    fprintf(OUTPUT_MODE, "Number of Processors detected by MPI: %d" , TotalPS);
		#endif
	}

	/* Make sure we have enough processors */
  	if (TotalPS < 2)
    {
	    fprintf(OUTPUT_MODE, "Error for Number of Processors: %d" , TotalPS);
      	fprintf(OUTPUT_MODE, " (Require minimum of 2).\n" );
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
      	MPI_Finalize(); 
      	exit(1);
	}

	return;
}


/*
 *********************************************************
    This function will search the array pointed to by
    searchBuf[] for the string searchString[] and return
    a pointer to the start of the searchString[] if found
    otherwise return NULL if not found.
*/
char *memstr(char *searchBuf, int searchBufSize, char *searchString, int searchStringSize)
{
	int i;
	int j;
	int notFound = 0;

	for (i=0; i < searchBufSize; i++)
	{
		if ((searchBufSize - i) < searchStringSize)
			break;

		if (searchBuf[i] == searchString[0])
		{
			notFound = 0;
			// we found first character now check rest
			for (j=1; j < searchStringSize; j++)
			{
				if (searchBuf[i+j] != searchString[j])
				{	notFound = 1;
					break;
				}
			}
			// we found the first occurance of the string so we are done
			if (notFound == 0)
			{
				#ifdef MPIBZIP2_DEBUG
				fprintf(OUTPUT_MODE, " memstr(): Found substring at byte: %d [%x]\n", i, &searchBuf[i]);
				#endif
				return &searchBuf[i];
			}
		}
	}

	return NULL;
}

/*
 *********************************************************
    This function tells all slaves to exit
*/
void kill_slaves()
{
	int i;

	for (i=1; i < TotalPS; i++)
	{
		// tell slaves to exit
 		MPI_Send(&allDone, 0, MPI_BYTE, i, 9999, MPI_COMM_WORLD);
	 		
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Master informed slave # %d to exit. \n", i );
		#endif
	}
}

/*
 *********************************************************
    This function works in two passes of the input file.
    The first pass will look for BZIP2 headers in the file
    and note their location and size of the sections.
    The second pass will read in those BZIP2 sections and
    pass them off the the selected CPU(s) for decompression.
 */
int producer_decompress(int hInfile, uint_special fileSize, queue *fifo)
{
	MPI_Status anmpistatus;
	std::vector <bz2BlockListing> bz2BlockList;
	outBuff TempOutBuffer;
	bz2BlockListing TempBlockListing;
	char *startPointer = NULL;
	char *BlockData = NULL;
	char *FileData = NULL;
	char bz2Header[] = {"BZh91AY&SY"};  // for 900k BWT block size
	uint_special bytesLeft = 0;
	uint_special inSize = 100000;
	uint_special currentByte = 0;
	uint_special startByte = 0;
	uint_special ret = 0;
	int sendret = -1;
	int BlockDataSize = 0;
	int rank_outstanding = 0;
	int blockNum = 0;
	int bz2NumBlocks = 0;
	int received;
	int receivedDataSize = 0;
	int receivedBlockNum = 0;
	int i;
	
	TempOutBuffer.buf = NULL;
	TempOutBuffer.bufSize = 0;

	// set search header to value in file
	bz2Header[3] = BWTblockSizeChar;

	// go to start of  file
	ret = lseek(hInfile, 0, SEEK_SET);
	if (ret != 0)
	{
		#ifdef _LARGEFILE64_SOURCE
		fprintf(OUTPUT_MODE, " *ERROR: Could not seek to beginning of file [%llu]!  Skipping...\n", ret);
		#else
		fprintf(OUTPUT_MODE, " *ERROR: Could not seek to beginning of file [%d]!  Skipping...\n", ret);
		#endif
		close(hInfile);
		allDone = 1;
		return -1;
	}

	// scan input file for BZIP2 block markers (BZh91AY&SY)
	pthread_mutex_lock(MemMutex);
	// allocate memory to store data to be transferred/received (max 9000k file blocks)
	BlockDataSize = (int) (((BlockSizeBytes*10)*1.01)+600);
	BlockData = new char[BlockDataSize];
	// allocate memory to read in file
	FileData = NULL;
	FileData = new char[inSize];
	pthread_mutex_unlock(MemMutex);
	// make sure memory was allocated properly
	if (BlockData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (BlockData)!  Skipping...\n");
		close(hInfile);
		allDone = 1;
		return -1;
	}
	if (FileData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (FileData)!  Skipping...\n");
		close(hInfile);
		allDone = 1;
		return -1;
	}

	// keep going until all the file is scanned for BZIP2 blocks
	bytesLeft = fileSize;
	while (bytesLeft > 0)
	{
		if (currentByte == 0)
		{
			#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, " -> Bytes To Read: %llu bytes...\n", inSize);
			#endif

			// read file data
			ret = read(hInfile, (char *) FileData, inSize);
		}
		else
		{
			// copy end section of previous buffer to new just in case the BZIP2 header is
			// located between two buffer boundaries
			memcpy(FileData, FileData+inSize-(strlen(bz2Header)-1), strlen(bz2Header)-1);
			#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, " -> Bytes To Read: %llu bytes...\n", inSize-(strlen(bz2Header)-1));
			#endif

			// read file data minus overflow from previous buffer
			ret = read(hInfile, (char *) FileData+strlen(bz2Header)-1, inSize-(strlen(bz2Header)-1));
		}
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " -> Total Bytes Read: %llu bytes...\n\n", ret);
		#endif
		if (ret < 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not read from file!  Skipping...\n");
			close(hInfile);
			pthread_mutex_lock(MemMutex);
			if (FileData != NULL)
				delete [] FileData;
			pthread_mutex_unlock(MemMutex);
			allDone = 1;
			return -1;
		}

		// scan buffer for bzip2 start header
		if (currentByte == 0)
			startPointer = memstr(FileData, ret, bz2Header, strlen(bz2Header));
		else
			startPointer = memstr(FileData, ret+(strlen(bz2Header)-1), bz2Header, strlen(bz2Header));
		while (startPointer != NULL)
		{
			if (currentByte == 0)
				startByte = startPointer - FileData + currentByte;
			else
				startByte = startPointer - FileData + currentByte - (strlen(bz2Header) - 1);
			#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, " Found substring at: %x\n", startPointer);
			fprintf(OUTPUT_MODE, " startByte = %llu\n", startByte);
			fprintf(OUTPUT_MODE, " bz2NumBlocks = %d\n", bz2NumBlocks);
			#endif

			// add data to end of block list
			TempBlockListing.dataStart = startByte;
			TempBlockListing.dataSize = 0;
			bz2BlockList.push_back(TempBlockListing);
			bz2NumBlocks++;
			
			if (currentByte == 0)
			{
				startPointer = memstr(startPointer+1, ret-(startPointer-FileData)-1, bz2Header, strlen(bz2Header));
			}
			else
			{
				startPointer = memstr(startPointer+1, ret-(startPointer-FileData)-1+(strlen(bz2Header)-1), bz2Header, strlen(bz2Header));
			}
		}

		currentByte += ret;
		bytesLeft -= ret;
	} // while
	
	pthread_mutex_lock(MemMutex);
	if (FileData != NULL)
		delete [] FileData;
	NumBlocks = bz2NumBlocks;

	pthread_mutex_lock(OutMutex);
	// create output buffer
	OutputBuffer.resize(bz2NumBlocks);
	// make sure memory was allocated properly
	if (OutputBuffer.size() != bz2NumBlocks)
	{
		fprintf(stderr, " *ERROR: Could not allocate memory (OutputBuffer)!  Aborting...\n");
		return 1;
	}
	// set empty buffer
	for (i=0; i < bz2NumBlocks; i++)
	{
		OutputBuffer[i].buf = NULL;
		OutputBuffer[i].bufSize = 0;
	}
	pthread_mutex_unlock(OutMutex);
	pthread_mutex_unlock(MemMutex);
	
	// calculate data sizes for each block
	for (i=0; i < bz2NumBlocks; i++)
	{
		if (i == bz2NumBlocks-1)
		{
			// special case for last block
			bz2BlockList[i].dataSize = fileSize - bz2BlockList[i].dataStart;
		}
		else if (i == 0)
		{
			// special case for first block
			bz2BlockList[i].dataSize = bz2BlockList[i+1].dataStart;
		}
		else
		{
			// normal case
			bz2BlockList[i].dataSize = bz2BlockList[i+1].dataStart - bz2BlockList[i].dataStart;
		}
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " bz2BlockList[%d].dataStart = %llu\n", i, bz2BlockList[i].dataStart);
		fprintf(OUTPUT_MODE, " bz2BlockList[%d].dataSize = %llu\n", i, bz2BlockList[i].dataSize);
		#endif
	}

	// keep going until all the blocks are processed
	for (i=0; i < bz2NumBlocks; i++)
	{
		// go to start of block position in file
		ret = lseek(hInfile, bz2BlockList[i].dataStart, SEEK_SET);
		if (ret != bz2BlockList[i].dataStart)
		{
			#ifdef _LARGEFILE64_SOURCE
			fprintf(OUTPUT_MODE, " *ERROR: Could not seek to beginning of file [%llu]!  Skipping...\n", ret);
			#else
			fprintf(OUTPUT_MODE, " *ERROR: Could not seek to beginning of file [%d]!  Skipping...\n", ret);
			#endif
			close(hInfile);
			allDone = 1;
			return -1;
		}

		// set buffer size
		inSize = bz2BlockList[i].dataSize;

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " -> Bytes To Read: %llu bytes...\n", inSize);
		#endif

		if (QuietMode != 1)
		{
			// give warning to user if block is larger than 250 million bytes
			if (inSize > 250000000)
			{
				#ifdef _LARGEFILE64_SOURCE
				fprintf(OUTPUT_MODE, " *WARNING: Compressed block size is large [%llu bytes].\n", inSize);
				#else
				fprintf(OUTPUT_MODE, " *WARNING: Compressed block size is large [%d bytes].\n", inSize);
				#endif
				fprintf(OUTPUT_MODE, "           If program aborts, use regular BZIP2 to decompress.\n");
			}
		}

		pthread_mutex_lock(MemMutex);
		// allocate memory to read in file
		FileData = NULL;
		FileData = new char[inSize];
		pthread_mutex_unlock(MemMutex);
		// make sure memory was allocated properly
		if (FileData == NULL)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (FileData)!  Skipping...\n");
			close(hInfile);
			allDone = 1;
			return -1;
		}

		// read file data
		ret = read(hInfile, (char *) FileData, inSize);
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " -> Total Bytes Read: %llu bytes...\n\n", ret);
		#endif
		// check to make sure all the data we expected was read in
		if (ret == 0)
		{
			pthread_mutex_lock(MemMutex);
			if (FileData != NULL)
			{
				delete [] FileData;
				FileData = NULL;
			}
			pthread_mutex_unlock(MemMutex);
			break;
		}
		else if (ret < 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not read from file [%d]!  Skipping...\n", errno);
			close(hInfile);
			pthread_mutex_lock(MemMutex);
			if (FileData != NULL)
				delete [] FileData;
			pthread_mutex_unlock(MemMutex);
			allDone = 1;
			return -1;
		}
		else if (ret != inSize)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not read enough data from file!  Skipping...\n");
			close(hInfile);
			pthread_mutex_lock(MemMutex);
			if (FileData != NULL)
				delete [] FileData;
			pthread_mutex_unlock(MemMutex);
			allDone = 1;
			return -1;
		}

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "producer:  Buffer: %x  Size: %llu   Block: %d\n", FileData, inSize, blockNum);
		#endif

	    /* Wait to receive idle signal from a rank */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Recv() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &receivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Get_count() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
     	
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Master received block # %d from Slave: %d  (%d bytes).\n", anmpistatus.MPI_TAG, anmpistatus.MPI_SOURCE, receivedDataSize);
		#endif

		// get info about block received from slave
		received = anmpistatus.MPI_SOURCE;
		receivedBlockNum = anmpistatus.MPI_TAG;

		// check if we have processed data to save to disk
     	if (receivedDataSize > 0)
     	{
	   		rank_outstanding--;
	   		if (rank_outstanding < 0)
	   			rank_outstanding = 0;

   			// add compressed data to output queue to write to disk
			pthread_mutex_lock(OutMutex);

			OutputBuffer[receivedBlockNum].buf = NULL;
			OutputBuffer[receivedBlockNum].buf = new char[receivedDataSize];
			// make sure memory was allocated properly
			if (OutputBuffer[receivedBlockNum].buf == NULL)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (CompressedData)!  Skipping...\n");
				allDone = 1;
				pthread_mutex_unlock(OutMutex);
				return -1;
			}
			memcpy(OutputBuffer[receivedBlockNum].buf, BlockData, receivedDataSize);
			OutputBuffer[receivedBlockNum].bufSize = receivedDataSize;
			
			pthread_mutex_unlock(OutMutex);
     	}		

   		/* Send work to idle rank */
 		sendret = -1;
 		sendret = MPI_Send(FileData, inSize, MPI_BYTE, received, blockNum, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not send data block # %d to slave # %d!  Skipping...\n", blockNum, received);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			allDone = 1;
			return -1;
 		}
 		
 		// keep track of how many ranks are processing data
 		rank_outstanding++;
 		
		#ifdef MPIBZIP2_DEBUG
   		fprintf(OUTPUT_MODE, "Master assigned block # %d to slave # %d .\n", blockNum, received);
 		#endif
 		
 		// reclaim memory
		pthread_mutex_lock(MemMutex);
		if (FileData != NULL)
			delete [] FileData;
		pthread_mutex_unlock(MemMutex);
		
		blockNum++;
	} // for
	
	close(hInfile);

	#ifdef MPIBZIP2_DEBUG
	fprintf(OUTPUT_MODE, "Master closed input file, total blocks: %d .\n", blockNum);
	#endif

	// wait for all slave ranks to report in
	while (rank_outstanding > 0)
	{
	    /* Wait to receive idle signal from a rank */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Recv() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &receivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Get_count() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}

		// get info about block received from slave
		received = anmpistatus.MPI_SOURCE;
		receivedBlockNum = anmpistatus.MPI_TAG;

		// check if we have processed data to save to disk
     	if (receivedDataSize > 0)
     	{
	   		rank_outstanding--;
	   		if (rank_outstanding < 0)
	   			rank_outstanding = 0;

	     	// add compressed data to output queue to write to disk
			pthread_mutex_lock(OutMutex);

			OutputBuffer[receivedBlockNum].buf = NULL;
			OutputBuffer[receivedBlockNum].buf = new char[receivedDataSize];
			// make sure memory was allocated properly
			if (OutputBuffer[receivedBlockNum].buf == NULL)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (CompressedData)!  Skipping...\n");
				allDone = 1;
				pthread_mutex_unlock(OutMutex);
				return -1;
			}
			memcpy(OutputBuffer[receivedBlockNum].buf, BlockData, receivedDataSize);
			OutputBuffer[receivedBlockNum].bufSize = receivedDataSize;
			
			pthread_mutex_unlock(OutMutex);
     	}		

		/* Send no more work signal to all slaves */
 		sendret = -1;
 		sendret = MPI_Send(BlockData, 0, MPI_BYTE, received, 0, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not send data NO MORE BLOCKS to slave # %d!  Skipping...\n", received);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			allDone = 1;
			return -1;
 		}
 		
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Master informed slave # %d no more blocks. \n", received );
		#endif
	}

	allDone = 1;

	return 0;
}

/*
 *********************************************************
 */
void consumer_decompress()
{
	MPI_Status anmpistatus;
	char *BlockData = NULL;
	char *DecompressedData = NULL;
	unsigned int inSize = 0;
	unsigned int outSize = 0;
	int BlockDataSize = 0;
	int ReceivedDataSize = 0;
	int blockNum = -1;
	int ret = -1;
	int sendret = -1;

	// allocate memory to store data to be transferred/received
	BlockDataSize = (int) (((BlockSizeBytes*10)*1.01)+600);
	BlockData = new char[BlockDataSize];
	// make sure memory was allocated properly
	if (BlockData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Slave %d could not allocate memory (BlockData)!\n", MyID);
		return;
	}

	/* Send idle signal to master */
	sendret = MPI_Send(BlockData, 0, MPI_BYTE, MasterID, 0, MPI_COMM_WORLD);
	if (sendret != MPI_SUCCESS)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send IDLE to Master!\n", MyID);
		fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
		fflush(OUTPUT_MODE);
		return;
 	}
	#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d reports to Master he's idle.\n", MyID);
	#endif

	while (allDone == 0) 
	{
     	/* Wait to receive work from master */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MasterID, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: Slave MPI_Recv() failed!\n");
			return;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &ReceivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Slave MPI_Get_count() failed!!\n");
			return;
     	}
     	
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d received block # %d  (%d bytes).\n", MyID, anmpistatus.MPI_TAG, ReceivedDataSize);
		#endif

		// check to see if there is any more work to do
		if ((ReceivedDataSize == 0) && (anmpistatus.MPI_TAG == 1234))
		{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks (wait for next file) instruction.\n", MyID);
			#endif
			
			/* Send idle signal to master for next file */
			sendret = MPI_Send(BlockData, 0, MPI_BYTE, MasterID, 0, MPI_COMM_WORLD);
			if (sendret != MPI_SUCCESS)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send IDLE to Master!\n", MyID);
				fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
				fflush(OUTPUT_MODE);
				return;
			}
			#ifdef MPIBZIP2_DEBUG
				fprintf(OUTPUT_MODE, "Slave #%d reports to Master he's idle.\n", MyID);
			#endif
			continue;
		}
		else if ((ReceivedDataSize == 0) && (anmpistatus.MPI_TAG == 9999))
		{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks (stop now) instruction.\n", MyID);
			#endif

			if (BlockData != NULL)
			{
				delete [] BlockData;
				BlockData = NULL;
			}
			return;
		}
     	else if (ReceivedDataSize == 0)
    	{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks instruction.\n", MyID);
			#endif
			
			continue;
    	}
   		
   		// assign values from received data
   		inSize = ReceivedDataSize;
   		blockNum = anmpistatus.MPI_TAG;

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d:  Buffer: %x  Size: %u   Block: %d\n", MyID, BlockData, inSize, blockNum);
		#endif

		outSize = (int) (((BlockSizeBytes*10)*1.01)+600);
		// allocate memory for decompressed data (start with space for 3600k file block size)
		DecompressedData = new char[outSize];
		// make sure memory was allocated properly
		if (DecompressedData == NULL)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (DecompressedData)!  Skipping...\n");
			return;
		}
	
		// decompress the memory buffer (verbose=0)
		ret = BZ2_bzBuffToBuffDecompress(DecompressedData, &outSize, BlockData, inSize, 0, Verbosity);
		if ((ret != BZ_OK) && (ret != BZ_OUTBUFF_FULL))
			fprintf(OUTPUT_MODE, " *ERROR during decompression: %d\n", ret);

		while (ret == BZ_OUTBUFF_FULL)
		{
			#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, "Increasing DecompressedData buffer size: %d -> %d\n", outSize, outSize*4);
			#endif

			if (DecompressedData != NULL)
				delete [] DecompressedData;
			DecompressedData = NULL;
			// increase buffer space
			outSize = outSize * 2;
			// allocate larger memory for decompressed data
			DecompressedData = new char[outSize];
			// make sure memory was allocated properly
			if (DecompressedData == NULL)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (DecompressedData)!  Skipping...\n");
				return;
			}

			// decompress the memory buffer (verbose=0)
			ret = BZ2_bzBuffToBuffDecompress(DecompressedData, &outSize, BlockData, inSize, 0, Verbosity);
			if ((ret != BZ_OK) && (ret != BZ_OUTBUFF_FULL))
				fprintf(OUTPUT_MODE, " *ERROR during decompression: %d\n", ret);
		} // while

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "\n Compressed Block Size: %u\n", inSize);
		fprintf(OUTPUT_MODE, "   Original Block Size: %u\n", outSize);
		#endif

		// send decompressed data back to master to be written to disk
 		sendret = MPI_Send(DecompressedData, outSize, MPI_BYTE, MasterID, blockNum, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send data block # %d to Master!\n", MyID, blockNum);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			return;
 		}
		#ifdef MPIBZIP2_DEBUG
			fprintf(stderr, "Slave #%d sends decompressed data back to Master and reports he's idle.\n", MyID);
		#endif

		if (DecompressedData != NULL)
		{
			delete [] DecompressedData;
			DecompressedData = NULL;
		}
	} // while
	#ifdef MPIBZIP2_DEBUG
	printf ("consumer: exiting\n");
	#endif

	return;
}

/*
 *********************************************************
 */
void *fileWriter(void *outname)
{
	char *OutFilename;
	uint_special CompressedSize = 0;
	int percentComplete = 0;
	int hOutfile = -1;
	int currBlock = 0;
	int ret = -1;

	OutFilename = (char *) outname;

	// write to file or stdout
	if (OutputStdOut == 0)
	{
		hOutfile = open(OutFilename, O_RDWR | O_CREAT | O_TRUNC | O_BINARY, FILE_MODE);
		// check to see if file creation was successful
		if (hOutfile == -1)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not create output file [%s]!\n", OutFilename);
			return (NULL);
		}
	}

	while ((currBlock < NumBlocks) || (allDone == 0))
	{
		if ((OutputBuffer.size() == 0) || (OutputBuffer[currBlock].bufSize < 1) || (OutputBuffer[currBlock].buf == NULL))
		{
			// sleep a little so we don't go into a tight loop using up all the CPU
			usleep(20000);
			continue;
		}
	
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "fileWriter:  Buffer: %x  Size: %u   Block: %d\n", OutputBuffer[currBlock].buf, OutputBuffer[currBlock].bufSize, currBlock);
		#endif

		if (OutputStdOut == 0)
		{
			// write data to the output file
			ret = write(hOutfile, OutputBuffer[currBlock].buf, OutputBuffer[currBlock].bufSize);
		}
		else
		{
			// write data to stdout
			ret = write(1, OutputBuffer[currBlock].buf, OutputBuffer[currBlock].bufSize);
		}
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "\n -> Total Bytes Written[%d]: %d bytes...\n", currBlock, ret);
		#endif
		CompressedSize += ret;
		if (ret <= 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: write() block: %d  ret: %d\n", currBlock, ret);
		}

		pthread_mutex_lock(MemMutex);
		if (OutputBuffer[currBlock].buf != NULL)
			delete [] OutputBuffer[currBlock].buf;
		pthread_mutex_unlock(MemMutex);
			
		currBlock++;
		// print current completion status
		if (QuietMode != 1)
		{
			percentComplete = 100 * currBlock / NumBlocks;
			fprintf(OUTPUT_MODE, "Completed: %d%%             \r", percentComplete);
			fflush(OUTPUT_MODE);
		}
	} // while

	if (OutputStdOut == 0)
		close(hOutfile);
	if (QuietMode != 1)
	{
		#ifdef _LARGEFILE64_SOURCE
		fprintf(OUTPUT_MODE, "    Output Size: %llu bytes\n", CompressedSize);
		#else
		fprintf(OUTPUT_MODE, "    Output Size: %u bytes\n", CompressedSize);
		#endif
	}

	return (NULL);
}

/*
 *********************************************************
 Producer/Master Function
 */
int producer(int hInfile, uint_special fileSize, int blockSize, queue *fifo)
{
	MPI_Status anmpistatus;
	char *BlockData = NULL;
	char *FileData = NULL;
	uint_special bytesLeft = 0;
	uint_special inSize = 0;
	int rank_outstanding = 0;
	int BlockDataSize = 0;
	int blockNum = 0;
	int ret = 0;
	int received = 0;
	int receivedDataSize = 0;
	int receivedBlockNum = 0;
	int sendret = -1;

	bytesLeft = fileSize;
	
	pthread_mutex_lock(MemMutex);
	// allocate memory to store data to be transferred/received
	BlockDataSize = (int) ((blockSize*1.01)+600);
	BlockData = new char[BlockDataSize];
	// allocate memory to read in file
	FileData = new char[blockSize];
	pthread_mutex_unlock(MemMutex);

	// make sure memory was allocated properly
	if (BlockData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (BlockData)!  Skipping...\n");
		close(hInfile);
		allDone = 1;
		return -1;
	}
	if (FileData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (FileData)!  Skipping...\n");
		close(hInfile);
		allDone = 1;
		return -1;
	}

	// keep going until all the file is processed
	while (bytesLeft > 0)
	{
		// set buffer size
		if (bytesLeft > blockSize)
			inSize = blockSize;
		else
			inSize = bytesLeft;

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " -> Bytes To Read: %llu bytes...\n", inSize);
		#endif

		// read file data
		ret = read(hInfile, (char *) FileData, inSize);
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, " -> Total Bytes Read: %d bytes...\n\n", ret);
		#endif
		if (ret == 0)
		{
			break;
		}
		else if (ret < 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not read from file!  Skipping...\n");
			close(hInfile);
			pthread_mutex_lock(MemMutex);
			if (FileData != NULL)
				delete [] FileData;
			pthread_mutex_unlock(MemMutex);
			allDone = 1;
			return -1;
		}
	
		// set bytes left after read
		bytesLeft -= ret;
		// check to make sure all the data we expected was read in
		if (ret != inSize)
			inSize = ret;

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "producer:  Buffer: %x  Size: %llu   Block: %d\n", FileData, inSize, blockNum);
		#endif

	    /* Wait to receive idle signal from a rank */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Recv() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &receivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Get_count() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
     	
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Master received block # %d from Slave: %d  (%d bytes).\n", anmpistatus.MPI_TAG, anmpistatus.MPI_SOURCE, receivedDataSize);
		#endif

		// get info about block received from slave
		received = anmpistatus.MPI_SOURCE;
		receivedBlockNum = anmpistatus.MPI_TAG;
   		
		// check if we have processed data to save to disk
     	if (receivedDataSize > 0)
     	{
	   		rank_outstanding--;
	   		if (rank_outstanding < 0)
	   			rank_outstanding = 0;

   			// add compressed data to output queue to write to disk
			pthread_mutex_lock(OutMutex);

			OutputBuffer[receivedBlockNum].buf = NULL;
			OutputBuffer[receivedBlockNum].buf = new char[receivedDataSize];
			// make sure memory was allocated properly
			if (OutputBuffer[receivedBlockNum].buf == NULL)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (CompressedData)!  Skipping...\n");
				allDone = 1;
				pthread_mutex_unlock(OutMutex);
				return -1;
			}
			memcpy(OutputBuffer[receivedBlockNum].buf, BlockData, receivedDataSize);
			OutputBuffer[receivedBlockNum].bufSize = receivedDataSize;
			
			pthread_mutex_unlock(OutMutex);
     	}		

   		/* Send work to idle rank */
 		sendret = -1;
 		sendret = MPI_Send(FileData, inSize, MPI_BYTE, received, blockNum, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not send data block # %d to slave # %d!  Skipping...\n", blockNum, received);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			allDone = 1;
			return -1;
 		}
 		
 		// keep track of how many ranks are processing data
 		rank_outstanding++;

		#ifdef MPIBZIP2_DEBUG
   		fprintf(OUTPUT_MODE, "Master assigned block # %d to slave # %d .\n", blockNum, received);
 		#endif
		
		blockNum++;
	} // while
	
	close(hInfile);

	#ifdef MPIBZIP2_DEBUG
	fprintf(OUTPUT_MODE, "Master closed input file, total blocks: %d .\n", blockNum);
	#endif

	// wait for all slave ranks to report in
	while (rank_outstanding > 0)
	{
	    /* Wait to receive idle signal from a rank */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Recv() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &receivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: MPI_Get_count() failed!  Code: %d\n", ret);
			allDone = 1;
			return -1;
     	}

		// get info about block received from slave
		received = anmpistatus.MPI_SOURCE;
		receivedBlockNum = anmpistatus.MPI_TAG;

		// check if we have processed data to save to disk
     	if (receivedDataSize > 0)
     	{
	   		rank_outstanding--;
	   		if (rank_outstanding < 0)
	   			rank_outstanding = 0;

	     	// add compressed data to output queue to write to disk
			pthread_mutex_lock(OutMutex);

			OutputBuffer[receivedBlockNum].buf = NULL;
			OutputBuffer[receivedBlockNum].buf = new char[receivedDataSize];
			// make sure memory was allocated properly
			if (OutputBuffer[receivedBlockNum].buf == NULL)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (CompressedData)!  Skipping...\n");
				allDone = 1;
				pthread_mutex_unlock(OutMutex);
				return -1;
			}
			memcpy(OutputBuffer[receivedBlockNum].buf, BlockData, receivedDataSize);
			OutputBuffer[receivedBlockNum].bufSize = receivedDataSize;
			
			pthread_mutex_unlock(OutMutex);
     	}		

		/* Send no more work signal to all slaves */
 		sendret = -1;
 		sendret = MPI_Send(BlockData, 0, MPI_BYTE, received, 0, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not send data NO MORE BLOCKS to slave # %d!  Skipping...\n", received);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			allDone = 1;
			return -1;
 		}
 		
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Master informed slave # %d no more blocks. \n", received );
		#endif
	}

	allDone = 1;

	#ifdef MPIBZIP2_DEBUG
	fprintf(OUTPUT_MODE, "Master finished receiving all compressed blocks...\n");
	#endif

	pthread_mutex_lock(MemMutex);
	if (FileData != NULL)
		delete [] FileData;
	pthread_mutex_unlock(MemMutex);

	return 0;
}

/*
 *********************************************************
 Consumer/Slave Function
 */
void consumer()
{
	MPI_Status anmpistatus;
	char *BlockData = NULL;
	char *CompressedData = NULL;
	unsigned int inSize = 0;
	unsigned int outSize = 0;
	int BlockDataSize = 0;
	int ReceivedDataSize = 0;
	int blockNum = -1;
	int ret = -1;
	int sendret = -1;

	// allocate memory to store data to be transferred/received
	BlockDataSize = (int) ((BlockSizeBytes*1.01)+600);
	BlockData = new char[BlockDataSize];
	// make sure memory was allocated properly
	if (BlockData == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Slave %d could not allocate memory (BlockData)!\n", MyID);
		return;
	}

	/* Send idle signal to master */
	sendret = MPI_Send(BlockData, 0, MPI_BYTE, MasterID, 0, MPI_COMM_WORLD);
	if (sendret != MPI_SUCCESS)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send IDLE to Master!\n", MyID);
		fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
		fflush(OUTPUT_MODE);
		return;
 	}
	#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d reports to Master he's idle.\n", MyID);
	#endif

	while (allDone == 0) 
	{
     	/* Wait to receive work from master */
     	ret = MPI_Recv(BlockData, BlockDataSize, MPI_BYTE, MasterID, MPI_ANY_TAG, MPI_COMM_WORLD, &anmpistatus);
     	if (ret != MPI_SUCCESS)
     	{
			fprintf(OUTPUT_MODE, " *ERROR: Slave MPI_Recv() failed!\n");
			return;
     	}
		ret = MPI_Get_count(&anmpistatus, MPI_BYTE, &ReceivedDataSize);
     	if (ret != MPI_SUCCESS)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Slave MPI_Get_count() failed!!\n");
			return;
     	}

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d received block # %d  (%d bytes).\n", MyID, anmpistatus.MPI_TAG, ReceivedDataSize);
		#endif
   		
		// check to see if there is any more work to do
		if ((ReceivedDataSize == 0) && (anmpistatus.MPI_TAG == 1234))
		{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks (wait for next file) instruction.\n", MyID);
			#endif
			
			/* Send idle signal to master for next file */
			sendret = MPI_Send(BlockData, 0, MPI_BYTE, MasterID, 0, MPI_COMM_WORLD);
			if (sendret != MPI_SUCCESS)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send IDLE to Master!\n", MyID);
				fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
				fflush(OUTPUT_MODE);
				return;
			}
			#ifdef MPIBZIP2_DEBUG
				fprintf(OUTPUT_MODE, "Slave #%d reports to Master he's idle.\n", MyID);
			#endif
			continue;
		}
		else if ((ReceivedDataSize == 0) && (anmpistatus.MPI_TAG == 9999))
		{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks (stop now) instruction.\n", MyID);
			#endif

			if (BlockData != NULL)
			{
				delete [] BlockData;
				BlockData = NULL;
			}

			return;
		}
     	else if (ReceivedDataSize == 0)
    	{
			#ifdef MPIBZIP2_DEBUG
	   		fprintf(OUTPUT_MODE, "Slave #%d received no more blocks instruction.\n", MyID);
			#endif
			
			continue;
    	}

   		// assign values from received data
   		inSize = ReceivedDataSize;
   		blockNum = anmpistatus.MPI_TAG;

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "Slave #%d:  Buffer: %x  Size: %u   Block: %d\n", MyID, BlockData, inSize, blockNum);
		#endif
		
		outSize = (int) ((inSize*1.01)+600);
		// allocate memory for compressed data
		CompressedData = new char[outSize];
		// make sure memory was allocated properly
		if (CompressedData == NULL)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (CompressedData)!  Skipping...\n");
			return;
		}

		// compress the memory buffer (blocksize=9*100k, verbose=0, worklevel=30)
		ret = BZ2_bzBuffToBuffCompress(CompressedData, &outSize, BlockData, inSize, BWTblockSize, Verbosity, 30);
		if (ret != BZ_OK)
			fprintf(OUTPUT_MODE, " *ERROR during compression: %d\n", ret);

		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "\n   Original Block Size: %u\n", inSize);
		fprintf(OUTPUT_MODE, " Compressed Block Size: %u\n", outSize);
		#endif

		// send compressed data back to master to be written to disk
 		sendret = MPI_Send(CompressedData, outSize, MPI_BYTE, MasterID, blockNum, MPI_COMM_WORLD);
 		if (sendret != MPI_SUCCESS)
 		{
			fprintf(OUTPUT_MODE, " *ERROR: Slave %d Could not send data block # %d to Master!\n", MyID, blockNum);
			fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
			fflush(OUTPUT_MODE);
			return;
 		}
		#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, "Slave #%d sends compressed data back to Master and reports he's idle.\n", MyID);
		#endif

		if (CompressedData != NULL)
		{
			delete [] CompressedData;
			CompressedData = NULL;
		}
	} // for
	#ifdef MPIBZIP2_DEBUG
	printf ("consumer %d: exiting\n", MyID);
	#endif
	
	return;
}

/*
 *********************************************************
 */
queue *queueInit(int queueSize)
{
	queue *q;
	QUEUESIZE = queueSize;

	q = new queue;
	if (q == NULL) 
		return NULL;
	
	q->buf = NULL;
	q->buf = new char*[queueSize];
	if (q->buf == NULL) 
		return NULL;

	q->bufSize = NULL;
	q->bufSize = new unsigned int[queueSize];
	if (q->bufSize == NULL) 
		return NULL;
	
	q->blockNum = NULL;
	q->blockNum = new int[queueSize];
	if (q->blockNum == NULL) 
		return NULL;

	q->empty = 1;
	q->full = 0;
	q->head = 0;
	q->tail = 0;
	
	q->mut = NULL;
	q->mut = new pthread_mutex_t;
	if (q->mut == NULL) 
		return NULL;
	pthread_mutex_init(q->mut, NULL);

	q->notFull = NULL;
	q->notFull = new pthread_cond_t;
	if (q->notFull == NULL) 
		return NULL;
	pthread_cond_init(q->notFull, NULL);

	q->notEmpty = NULL;
	q->notEmpty = new pthread_cond_t;
	if (q->notEmpty == NULL) 
		return NULL;
	pthread_cond_init(q->notEmpty, NULL);

	return (q);
}

/*
 *********************************************************
 */
void queueDelete (queue *q)
{
	if (q == NULL)
		return;

	if (q->mut != NULL) 
	{
		pthread_mutex_destroy(q->mut);
		delete q->mut;
		q->mut = NULL;
	}

	if (q->notFull != NULL) 
	{
		pthread_cond_destroy(q->notFull);
		delete q->notFull;
		q->notFull = NULL;
	}

	if (q->notEmpty != NULL) 
	{
		pthread_cond_destroy(q->notEmpty);
		delete q->notEmpty;
		q->notEmpty = NULL;
	}

    delete [] q->blockNum;
    delete [] q->bufSize;
    delete [] q->buf;
	delete q;
	q = NULL;

	return;
}

/*
 *********************************************************
 */
void queueAdd (queue *q, char *in, unsigned int bufSize, int blockNum)
{
	q->buf[q->tail] = in;
	q->bufSize[q->tail] = bufSize;
	q->blockNum[q->tail] = blockNum;
	q->tail++;
	if (q->tail == QUEUESIZE)
		q->tail = 0;
	if (q->tail == q->head)
		q->full = 1;
	q->empty = 0;

	return;
}

/*
 *********************************************************
 */
char *queueDel (queue *q, unsigned int *bufSize, int *blockNum)
{
	char *out;
	
	out = q->buf[q->head];
	*bufSize = q->bufSize[q->head];
	*blockNum = q->blockNum[q->head];

	q->head++;
	if (q->head == QUEUESIZE)
		q->head = 0;
	if (q->head == q->tail)
		q->empty = 1;
	q->full = 0;

	return out;
}

/*
 *********************************************************
 Much of the code in this function is taken from bzip2.c
 */
int testBZ2ErrorHandling(int bzerr, BZFILE* bzf, int streamNo)
{
	int bzerr_dummy;

	BZ2_bzReadClose(&bzerr_dummy, bzf);
	switch (bzerr) 
	{
		case BZ_CONFIG_ERROR:
			fprintf(OUTPUT_MODE, " *ERROR: Integers are not the right size for libbzip2. Aborting!\n");
			exit(3);
			break;
		case BZ_IO_ERROR:
			fprintf(OUTPUT_MODE, " *ERROR: Integers are not the right size for libbzip2. Aborting!\n");
			return 1;
			break;
		case BZ_DATA_ERROR:
			fprintf(OUTPUT_MODE,	" *ERROR: Data integrity (CRC) error in data!  Skipping...\n");
			return -1;
			break;
		case BZ_MEM_ERROR:
			fprintf(OUTPUT_MODE, " *ERROR: Could NOT allocate enough memory. Aborting!\n");
			return 1;
			break;
		case BZ_UNEXPECTED_EOF:
			fprintf(OUTPUT_MODE,	" *ERROR: File ends unexpectedly!  Skipping...\n");
			return -1;
			break;
		case BZ_DATA_ERROR_MAGIC:
			if (streamNo == 1)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Bad magic number (file not created by bzip2)!  Skipping...\n");
				return -1;
			}
			else
			{
				if (QuietMode != 1)
					fprintf(OUTPUT_MODE, " *Warning: Trailing garbage after EOF ignored!\n");
				return 0;
			}
		default:
			fprintf(OUTPUT_MODE, " *ERROR: Unexpected error. Aborting!\n");
			exit(3);
	}

	return 0;
}

/*
 *********************************************************
 Much of the code in this function is taken from bzip2.c
 */
int testCompressedData(char *fileName)
{
	FILE *zStream = NULL;
	int ret = 0;

	BZFILE* bzf = NULL;
	unsigned char obuf[5000];
	unsigned char unused[BZ_MAX_UNUSED];
	unsigned char *unusedTmp;
	int bzerr, nread, streamNo;
	int nUnused;
	int i;

	nUnused = 0;
	streamNo = 0;

	// open the file for reading
    zStream = fopen(fileName, "rb");
    if (zStream == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not open input file [%s]!  Skipping...\n", fileName);
		return -1;
	}

	// check file stream for errors
	if (ferror(zStream))
	{
		fprintf(OUTPUT_MODE, " *ERROR: Problem with stream of file [%s]!  Skipping...\n", fileName);
		fclose(zStream);
		return -1;
	}

	// loop until end of file
	while(true)
	{
		bzf = BZ2_bzReadOpen(&bzerr, zStream, Verbosity, 0, unused, nUnused);
		if (bzf == NULL || bzerr != BZ_OK)
		{
			ret = testBZ2ErrorHandling(bzerr, bzf, streamNo);
			fclose(zStream);
			return ret;
		}

		streamNo++;

		while (bzerr == BZ_OK)
		{
			nread = BZ2_bzRead(&bzerr, bzf, obuf, 5000);
			if (bzerr == BZ_DATA_ERROR_MAGIC)
			{
				ret = testBZ2ErrorHandling(bzerr, bzf, streamNo);
				fclose(zStream);
				return ret;
			}
		}
		if (bzerr != BZ_STREAM_END)
		{
			ret = testBZ2ErrorHandling(bzerr, bzf, streamNo);
			fclose(zStream);
			return ret;
		}

		BZ2_bzReadGetUnused(&bzerr, bzf, (void**)(&unusedTmp), &nUnused);
		if (bzerr != BZ_OK)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Unexpected error. Aborting!\n");
			exit(3);
		}

		for (i = 0; i < nUnused; i++)
			unused[i] = unusedTmp[i];

		BZ2_bzReadClose(&bzerr, bzf);
		if (bzerr != BZ_OK)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Unexpected error. Aborting!\n");
			exit(3);
		}

		// check to see if we are at the end of the file
		if (nUnused == 0)
		{
			int c = fgetc(zStream);
			if (c == EOF)
				break;
			else
				ungetc(c, zStream);
		}
	}

	// check file stream for errors
	if (ferror(zStream))
	{
		fprintf(OUTPUT_MODE, " *ERROR: Problem with stream of file [%s]!  Skipping...\n", fileName);
		fclose(zStream);
		return -1;
	}

	// close file
	ret = fclose(zStream);
	if (ret == EOF)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Problem closing file [%s]!  Skipping...\n", fileName);
		return -1;
	}

	return 0;
}

/*
 *********************************************************
 */
int getFileMetaData(char *fileName)
{
	int ret = 0;

	// get the file meta data and store it in the global structure	
	ret = stat(fileName, &fileMetaData);

	return ret;
}

/*
 *********************************************************
 */
int writeFileMetaData(char *fileName)
{
	int ret = 0;
	struct utimbuf uTimBuf;

	// store file times in structure
	uTimBuf.actime = fileMetaData.st_atime;
	uTimBuf.modtime = fileMetaData.st_mtime;

	// update file with stored file permissions
	ret = chmod(fileName, fileMetaData.st_mode);
	if (ret != 0)
		return ret;

	// update file with stored file access and modification times
	ret = utime(fileName, &uTimBuf);
	if (ret != 0)
		return ret;

	// update file with stored file ownership (if access allows)
	chown(fileName, fileMetaData.st_uid, fileMetaData.st_gid);

	return 0;
}

/*
 *********************************************************
 */
void banner()
{
	fprintf(OUTPUT_MODE, "MPI BZIP2 v0.6  -  by: Jeff Gilchrist [http://compression.ca]\n");
	fprintf(OUTPUT_MODE, "[Jul. 18, 2007]        (uses libbzip2  by Julian Seward)\n\n");
	fprintf(OUTPUT_MODE, "** This is a BETA version - Use at your own risk! **\n");

	return;
}

/*
 *********************************************************
 */
void usage(char* progname)
{
	if (MyID == MasterID) 
	{
		banner();
		fprintf(OUTPUT_MODE, "\nInvalid command line!  Aborting...\n\n");
		fprintf(OUTPUT_MODE, "Usage: %s [-1 .. -9] [-b#cdfktvV] <filename> <filename2> <filenameN>\n", progname);
		fprintf(OUTPUT_MODE, " -b#      : where # is the file block size in 100k (default 9 = 900k)\n");
		fprintf(OUTPUT_MODE, " -c       : output to standard out (stdout)\n");
		fprintf(OUTPUT_MODE, " -d       : decompress file\n");
		fprintf(OUTPUT_MODE, " -f       : force, overwrite existing output file\n");
		fprintf(OUTPUT_MODE, " -k       : keep input file, don't delete\n");
		fprintf(OUTPUT_MODE, " -t       : test compressed file integrity\n");
        fprintf(OUTPUT_MODE, " -v       : verbose mode\n");
		fprintf(OUTPUT_MODE, " -V       : display version info for mpibzip2 then exit\n");
		fprintf(OUTPUT_MODE, " -1 .. -9 : set BWT block size to 100k .. 900k (default 900k)\n\n");
		fprintf(OUTPUT_MODE, "Example: mpibzip2 -b15k myfile.tar\n");
		fprintf(OUTPUT_MODE, "Example: mpibzip2 -v -5 myfile.tar second*.txt\n");
		fprintf(OUTPUT_MODE, "Example: mpibzip2 -d myfile.tar.bz2\n\n");
	}

	// wait for all slave MPI ranks to get here before exiting
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	exit(-1);
}

/*
 *********************************************************
 */
int main(int argc, char* argv[])
{
	queue *fifo;
	pthread_t output;
	char **FileList = NULL;
	char *InFilename = NULL;
	char bz2Header[] = {"BZh91AY&SY"};  // using 900k block size
	char OutFilename[255];
	char cmdLineTemp[255];
	char tmpBuff[50];
	struct timeval tvStartTime;
	struct timeval tvStopTime;
	struct timezone tz;
	struct stat statbuf;
	double timeCalc = 0.0;
	double timeStart = 0.0;
	double timeStop = 0.0;
	uint_special fileSize = 0;
	int cmdLineTempCount = 0;
	int numCPU = 2;
	int hInfile = -1;
	int hOutfile = -1;
	int numBlocks = 0;
	int blockSize = 9*100000;
	int decompress = 0;
	int testFile = 0;
	int keep = 0;
	int force = 0;
	int ret = 0;
	int sendret = 0;
	int i, j, k;
	int fileLoop;
	int errLevel = 0;

	// Initialize MPI
	MPI_Init(&argc,&argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &MyID);
	MPI_Comm_size(MPI_COMM_WORLD, &TotalPS);
	MasterSlaveModelInitialize();

	// check to make sure proper command line is given
	if (argc < 2)
	{
		usage(argv[0]);
	}
	
	FileListCount = 0;
	FileList = new char *[argc];
	if (FileList == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Not enough memory!  Aborting...\n");
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		return 1;
	}
	
	// Set number of CPUs being used
	numCPU = TotalPS;

	// parse command line switches	
	for (i=1; i < argc; i++)
	{
		if (argv[i][0] == '-')
		{
			#ifdef MPIBZIP2_DEBUG
			fprintf(OUTPUT_MODE, "argv[%d]: %s   Len: %d\n", i, argv[i], strlen(argv[i]));
			#endif
			// check for multiple switches grouped together
	        for (j=1; argv[i][j] != '\0'; j++) 
	        {
				switch (argv[i][j])
				{
				case 'b': k = j+1; cmdLineTempCount = 0; strcpy(cmdLineTemp, "9"); blockSize = 900000;
						  while (argv[i][k] != '\0' && k < sizeof(cmdLineTemp))
						  {
							// no more numbers, finish
							if ((argv[i][k] < '0') || (argv[i][k] > '9'))
								break;
							k++;
							cmdLineTempCount++;
						  }
						  if (cmdLineTempCount == 0)
							usage(argv[0]);
						  strncpy(cmdLineTemp, argv[i]+j+1, cmdLineTempCount);
						  blockSize = atoi(cmdLineTemp)*100000;
						  if ((blockSize < 100000) || (blockSize > 9000000))
						  {
							fprintf(stderr," *ERROR: File block size Min: 100k and Max: 9000k!  Aborting...\n");
							// wait for all slave MPI ranks to get here before exiting
							MPI_Barrier(MPI_COMM_WORLD);
							MPI_Finalize();
							return 1;
						  }
						  j += cmdLineTempCount;
						  #ifdef MPIBZIP2_DEBUG
						  fprintf(OUTPUT_MODE, "-b%d\n", blockSize);
						  #endif
						  break;
				case 'd': decompress = 1; break;
				case 'c': OutputStdOut = 1; keep = 1; break;
				case 'f': force = 1; break;
				case 'k': keep = 1; break;
				case 't': testFile = 1; break;
				case 'v': QuietMode = 0; break;
				case 'V': if (MyID == MasterID)
						    banner();
						  // wait for all slave MPI ranks to get here before exiting
						  MPI_Barrier(MPI_COMM_WORLD);
						  MPI_Finalize();
						  exit(0); break;
				case '1': BWTblockSize = 1; break;
				case '2': BWTblockSize = 2; break;
				case '3': BWTblockSize = 3; break;
				case '4': BWTblockSize = 4; break;
				case '5': BWTblockSize = 5; break;
				case '6': BWTblockSize = 6; break;
				case '7': BWTblockSize = 7; break;
				case '8': BWTblockSize = 8; break;
				case '9': BWTblockSize = 9; break;
				}
			}
		}
		else
		{
			// add filename to list for processing FileListCount
			FileList[FileListCount] = argv[i];
			FileListCount++;
		}
	} /* for */
	
	if (FileListCount == 0)
		usage(argv[0]);

	// set global block size
	BlockSizeBytes = blockSize;

	// Determine if we are a Master or a Slave rank
	if (MyID != MasterID) 
	{
		// only activate slave ranks if we have work for them to do
		if (testFile == 0)
		{
			if (decompress == 1)
				consumer_decompress();
			else
				consumer();
		}

		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		return 0;
	}

	if (QuietMode != 1)
	{
		// display program banner
		banner();
	}

	// setup signal handling
	sigInFilename = NULL;
	sigOutFilename = NULL;
	signal(SIGINT,  mySignalCatcher);
	signal(SIGTERM, mySignalCatcher);
	signal(SIGHUP,  mySignalCatcher);
	
	// display global settings
	if (QuietMode != 1)
	{
		if (testFile != 1)
		{
			fprintf(OUTPUT_MODE, "\n         # CPUs: 1 Master, %d Slaves\n", numCPU-1);
			if (decompress != 1)
			{
				fprintf(OUTPUT_MODE, " BWT Block Size: %d00k\n", BWTblockSize);
				if (blockSize < 100000)
					fprintf(OUTPUT_MODE, "File Block Size: %d bytes\n", blockSize);
				else
					fprintf(OUTPUT_MODE, "File Block Size: %dk\n", blockSize/1000);
			}
		}
		fprintf(OUTPUT_MODE, "-------------------------------------------\n");
	}

	// initialize mutexes
	OutMutex = new pthread_mutex_t;
	// make sure memory was allocated properly
	if (OutMutex == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (OutMutex)!  Aborting...\n");
		kill_slaves();
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		return 1;
	}
	pthread_mutex_init(OutMutex, NULL);

	MemMutex = new pthread_mutex_t;
	// make sure memory was allocated properly
	if (MemMutex == NULL)
	{
		fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (MemMutex)!  Aborting...\n");
		kill_slaves();
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		return 1;
	}
	pthread_mutex_init(MemMutex, NULL);

	// create queue
	fifo = queueInit(numCPU);
	if (fifo == NULL)
	{
		fprintf (OUTPUT_MODE, " *ERROR: Queue Init failed.  Aborting...\n");
		kill_slaves();
		// wait for all slave MPI ranks to get here before exiting
		MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
		return 1;
	}
	
	// process all files
	for (fileLoop=0; fileLoop < FileListCount; fileLoop++)
	{
		// get current time for benchmark reference	
		gettimeofday(&tvStartTime, &tz);

		allDone = 0;

		// set input filename
		InFilename = FileList[fileLoop];

		// test file for errors if requested
		if (testFile != 0)
		{
			if (QuietMode != 1)
			{
				fprintf(OUTPUT_MODE, "      File #: %d of %d\n", fileLoop+1, FileListCount);
				fprintf(OUTPUT_MODE, "     Testing: %s\n", InFilename);
			}
			ret = testCompressedData(InFilename);
			if (ret > 0)
			{
				kill_slaves();
				// wait for all slave MPI ranks to get here before exiting
				MPI_Barrier(MPI_COMM_WORLD);
				MPI_Finalize();
				return ret;
			}
			else if (ret == 0)
			{
				if (QuietMode != 1)
					fprintf(OUTPUT_MODE, "        Test: OK\n");
			}
			else
				errLevel = 2;

			if (QuietMode != 1)
				fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}

		// set ouput filename
		strncpy(OutFilename, FileList[fileLoop], 250);
		if (decompress == 1)
		{
			// check if input file is a valid .bz2 compressed file
			hInfile = open(InFilename, O_RDONLY | O_BINARY);
			// check to see if file exists before processing
			if (hInfile == -1)
			{
				fprintf(OUTPUT_MODE, " *ERROR: File [%s] NOT found!  Skipping...\n", InFilename);
				fprintf(OUTPUT_MODE, "-------------------------------------------\n");
				continue;
			}
			memset(tmpBuff, 0, sizeof(tmpBuff));
			ret = read(hInfile, tmpBuff, strlen(bz2Header)+1);
			close(hInfile);
			if ((ret == -1) || (ret < strlen(bz2Header)+1))
			{
				fprintf(OUTPUT_MODE, " *ERROR: File [%s] is NOT a valid bzip2!  Skipping...\n", InFilename);
				fprintf(OUTPUT_MODE, "-------------------------------------------\n");
				continue;
			}
			else
			{
				// make sure start of file has valid bzip2 header
				if (memstr(tmpBuff, 4, bz2Header, 3) == NULL)
				{
					fprintf(OUTPUT_MODE, " *ERROR: File [%s] is NOT a valid bzip2!  Skipping...\n", InFilename);
					fprintf(OUTPUT_MODE, "-------------------------------------------\n");
					continue;
				}
				// skip 4th char which differs depending on BWT block size used
				if (memstr(tmpBuff+4, ret-4, bz2Header+4, strlen(bz2Header)-4) == NULL)
				{
					fprintf(OUTPUT_MODE, " *ERROR: File [%s] is NOT a valid bzip2!  Skipping...\n", InFilename);
					fprintf(OUTPUT_MODE, "-------------------------------------------\n");
					continue;
				}
				// set block size for decompression
				if ((tmpBuff[3] >= '1') && (tmpBuff[3] <= '9'))
					BWTblockSizeChar = tmpBuff[3];
				else
				{
					fprintf(OUTPUT_MODE, " *ERROR: File [%s] is NOT a valid bzip2!  Skipping...\n", InFilename);
					fprintf(OUTPUT_MODE, "-------------------------------------------\n");
					continue;
				}
			}

			// check if filename ends with .bz2
			if (strncasecmp(&OutFilename[strlen(OutFilename)-4], ".bz2", 4) == 0)
			{
				// remove .bz2 extension
				OutFilename[strlen(OutFilename)-4] = '\0';
			}
			else
			{
				// add .out extension so we don't overwrite original file
				strcat(OutFilename, ".out");
			}
		}
		else
		{
			// check input file to make sure its not already a .bz2 file
			if (strncasecmp(&InFilename[strlen(InFilename)-4], ".bz2", 4) == 0)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Input file [%s] already has a .bz2 extension!  Skipping...\n", InFilename);
				fprintf(OUTPUT_MODE, "-------------------------------------------\n");
				continue;
			}
			strcat(OutFilename, ".bz2");
		}
	
		// setup signal handling filenames
		sigInFilename = InFilename;
		sigOutFilename = OutFilename;
		
		// read file for compression
		hInfile = open(InFilename, O_RDONLY | O_BINARY);
		// check to see if file exists before processing
		if (hInfile == -1)
		{
			fprintf(OUTPUT_MODE, " *ERROR: File [%s] NOT found!  Skipping...\n", InFilename);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}

		// get some information about the file
		fstat(hInfile, &statbuf);
		// check to make input is not a directory
		if (S_ISDIR(statbuf.st_mode))
		{
			fprintf(OUTPUT_MODE, " *ERROR: File [%s] is a directory!  Skipping...\n", InFilename);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}
		// check to make sure input is a regular file
		if (!S_ISREG(statbuf.st_mode))
		{
			fprintf(OUTPUT_MODE, " *ERROR: File [%s] is not a regular file!  Skipping...\n", InFilename);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}
		// get size of file
		fileSize = statbuf.st_size;
		// don't process a 0 byte file
		if (fileSize == 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: File is of size 0 [%s]!  Skipping...\n", InFilename);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}

		// get file meta data to write to output file
		if (getFileMetaData(InFilename) != 0)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not get file meta data from [%s]!  Skipping...\n", InFilename);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
			continue;
		}

		// check to see if output file exists
		if ((force != 1) && (OutputStdOut == 0))
		{
			hOutfile = open(OutFilename, O_RDONLY | O_BINARY);
			// check to see if file exists before processing
			if (hOutfile != -1)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Output file [%s] already exists!  Use -f to overwrite...\n", OutFilename);
				fprintf(OUTPUT_MODE, "-------------------------------------------\n");
				close(hOutfile);
				continue;
			}
		}

		// if we have more than 1 file to process, get slaves ready for next file
		if (fileLoop > 0)
		{
			for (i=1; i < TotalPS; i++)
			{
				// Restart slaves for next file
		 		sendret = -1;
		 		sendret = MPI_Send(&allDone, 0, MPI_BYTE, i, 1234, MPI_COMM_WORLD);
		 		if (sendret != MPI_SUCCESS)
		 		{
					fprintf(OUTPUT_MODE, " *ERROR: Could not send data RESTART to slave # %d!  Skipping...\n", i);
					fprintf(OUTPUT_MODE, "         MPI_Send error code: %d\n", sendret);
					fflush(OUTPUT_MODE);
					kill_slaves();
					// wait for all slave MPI ranks to get here before exiting
					MPI_Barrier(MPI_COMM_WORLD);
					MPI_Finalize();
					return 1;
		 		}
		 		
				#ifdef MPIBZIP2_DEBUG
				fprintf(OUTPUT_MODE, "Master informed slave # %d to wait for next file. \n", i );
				#endif
			}
		}

		// display per file settings
		if (QuietMode != 1)
		{
			fprintf(OUTPUT_MODE, "         File #: %d of %d\n", fileLoop+1, FileListCount);
			fprintf(OUTPUT_MODE, "     Input Name: %s\n", InFilename);
			if (OutputStdOut == 0)
				fprintf(OUTPUT_MODE, "    Output Name: %s\n\n", OutFilename);
			else
				fprintf(OUTPUT_MODE, "    Output Name: <stdout>\n\n");
		
			if (decompress == 1)
				fprintf(OUTPUT_MODE, " BWT Block Size: %c00k\n", BWTblockSizeChar);

			#ifdef _LARGEFILE64_SOURCE
			fprintf(OUTPUT_MODE, "     Input Size: %llu bytes\n", fileSize);
			#else
			fprintf(OUTPUT_MODE, "     Input Size: %u bytes\n", fileSize);
			#endif
		}
		
		if (decompress == 1)
		{
			numBlocks = 0;
			BlockSizeBytes = (BWTblockSizeChar - 48) * 100000;
		}
		else
		{
			// calculate the # of blocks of data
			numBlocks = (fileSize + blockSize - 1) / blockSize;
		}
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "# Blocks: %d\n", numBlocks);
		#endif
		// set global variable
		NumBlocks = numBlocks;
		// create output buffer
		OutputBuffer.resize(numBlocks);
		// make sure memory was allocated properly
		if (OutputBuffer.size() != numBlocks)
		{
			fprintf(OUTPUT_MODE, " *ERROR: Could not allocate memory (OutputBuffer)!  Aborting...\n");
			kill_slaves();
			// wait for all slave MPI ranks to get here before exiting
			MPI_Barrier(MPI_COMM_WORLD);
			MPI_Finalize();
			return 1;
		}
		// set empty buffer
		for (i=0; i < numBlocks; i++)
		{
			OutputBuffer[i].buf = NULL;
			OutputBuffer[i].bufSize = 0;
		}
		
		if (decompress == 1)
		{
			if (QuietMode != 1)
				fprintf(OUTPUT_MODE, "Decompressing data...\n");
	
			ret = pthread_create(&output, NULL, fileWriter, OutFilename);
			if (ret != 0)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Not enough resources to create fileWriter thread (code = %d)  Aborting...\n", ret);	
				kill_slaves();
				// wait for all slave MPI ranks to get here before exiting
				MPI_Barrier(MPI_COMM_WORLD);
				MPI_Finalize();
				return 1;
			}
	
			// start reading in data for decompression
			sendret = producer_decompress(hInfile, fileSize, fifo);
			if (sendret != 0)
			{
				kill_slaves();
				// wait for all slave MPI ranks to get here before exiting
				MPI_Barrier(MPI_COMM_WORLD);
				MPI_Finalize();
				return 1;
			}
		}
		else
		{
			if (QuietMode != 1)
				fprintf(OUTPUT_MODE, "Compressing data...\n");
	
			ret = pthread_create(&output, NULL, fileWriter, OutFilename);
			if (ret != 0)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Not enough resources to create fileWriter thread (code = %d)  Aborting...\n", ret);	
				kill_slaves();
				// wait for all slave MPI ranks to get here before exiting
				MPI_Barrier(MPI_COMM_WORLD);
				MPI_Finalize();
				return 1;
			}

			// start reading in data for compression
			sendret = producer(hInfile, fileSize, blockSize, fifo);
			if (sendret != 0)
			{
				kill_slaves();
				// wait for all slave MPI ranks to get here before exiting
				MPI_Barrier(MPI_COMM_WORLD);
				MPI_Finalize();
				return 1;
			}
		}
	
		// wait until exit of thread
		pthread_join(output, NULL);

		if (OutputStdOut == 0)
		{
			statbuf.st_size = 0;
			// check to see if output file is larger than 0 bytes
			stat(OutFilename, &statbuf);
			// get size of file
			if (statbuf.st_size == 0)
			{
				fprintf(OUTPUT_MODE, " *ERROR: Output file is size 0 bytes!  Deleting...\n");
				remove(OutFilename);
			}
			else
			{
				// write store file meta data to output file
				if (writeFileMetaData(OutFilename) != 0)
					fprintf(OUTPUT_MODE, " *ERROR: Could not write file meta data to [%s]!\n", InFilename);
			}
		}

		// finished processing file
		sigInFilename = NULL;
		sigOutFilename = NULL;

		// remove input file unless requested not to by user
		if (keep != 1)
		{
			if (OutputStdOut == 0)
			{
				// only remove input file if output file exists
				if (stat(OutFilename, &statbuf) == 0)
					remove(InFilename);
			}
			else
				remove(InFilename);
		}
		
		// reclaim memory
		OutputBuffer.clear();
		fifo->empty = 1;
		fifo->full = 0;
		fifo->head = 0;
		fifo->tail = 0;

		// get current time for end of benchmark
		gettimeofday(&tvStopTime, &tz);
	
		#ifdef MPIBZIP2_DEBUG
		fprintf(OUTPUT_MODE, "\n Start Time: %ld + %ld\n", tvStartTime.tv_sec, tvStartTime.tv_usec);
		fprintf(OUTPUT_MODE, " Stop Time : %ld + %ld\n", tvStopTime.tv_sec, tvStopTime.tv_usec);
		#endif
	
		// convert time structure to real numbers
		timeStart = (double)tvStartTime.tv_sec + ((double)tvStartTime.tv_usec / 1000000);
		timeStop = (double)tvStopTime.tv_sec + ((double)tvStopTime.tv_usec / 1000000);
		timeCalc = timeStop - timeStart;
		if (QuietMode != 1)
		{
			fprintf(OUTPUT_MODE, "     Wall Clock: %f seconds\n", timeCalc);
			fprintf(OUTPUT_MODE, "-------------------------------------------\n");
		}
	} /* for */

	// make sure all slaves exit
	kill_slaves();

	// reclaim memory
	queueDelete(fifo);
	fifo = NULL;
	if (OutMutex != NULL)
	{
		pthread_mutex_destroy(OutMutex);
		delete OutMutex;
		OutMutex = NULL;
	}
	if (MemMutex != NULL)
	{
		pthread_mutex_destroy(MemMutex);
		delete MemMutex;
		MemMutex = NULL;
	}

	// wait for all MPI ranks before exiting
	MPI_Barrier(MPI_COMM_WORLD);
	MPI_Finalize();
	
	return errLevel;
}
