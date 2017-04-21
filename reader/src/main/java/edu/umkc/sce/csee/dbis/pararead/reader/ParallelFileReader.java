package edu.umkc.sce.csee.dbis.pararead.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class ParallelFileReader {

  private final String filename;
  private final int numSplits;
  private final long[] readOffset;

  public ParallelFileReader(String filename, int numSplits) {
    this.filename = filename;
    this.numSplits = numSplits;

    // Get the read offset for every thread
    readOffset = getReadOffsets(filename, numSplits);
  }

  public void readTheFile(int[] lineCounters) {
    // Read the file via threads, each starting at a readOffset
    final int numOffsets = readOffset.length;
    int offset = 0;
    Thread [] threads = new Thread[numSplits];
    while (offset < numOffsets - 1) {
      Thread t = new ReadSplitThread(filename, readOffset[offset], readOffset[offset + 1],lineCounters);
      t.start();
      threads[offset] = t;
      offset++;
    }
    
    for (int t = 0; t < threads.length; t++){
      try {
        threads[t].join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    
  }

  private long[] getReadOffsets(String filename, int numSplits) {
    // Get split offsets
    final long[] rawReadOffset = getRawReadOffset(filename, numSplits);
    // System.out.println(Arrays.toString(rawReadOffset));

    // Align split offsets to the beginning of the lines
    final long[] readOffsets = getFormattedReadOffset(filename, rawReadOffset);
    // System.out.println(Arrays.toString(readOffsets));

    return readOffsets;
  }

  private long[] getFormattedReadOffset(String filename, long[] rawReadOffset) {
    final int numOffsets = rawReadOffset.length;
    long[] readOffsets = new long[numOffsets + 1];
    readOffsets[0] = (long) 0; // always start from the beginning of file
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(filename, "r");
      readOffsets[numOffsets] = raf.length(); // last offset is the end of the file
      for (int ofs = 1; ofs < numOffsets; ofs++) {

        raf.seek(rawReadOffset[ofs]); // go to offset
        raf.readLine(); // align to new line
        readOffsets[ofs] = raf.getFilePointer(); // get this line's offset
      }
      raf.close();
      return readOffsets;

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return new long[] {(long) 0};
  }

  private long[] getRawReadOffset(String filename, int numSplits) {
    RandomAccessFile raf;
    long fileLength = 0;
    try {
      raf = new RandomAccessFile(filename, "r");
      fileLength = raf.length();
      raf.close();
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    final long splitLength = fileLength / numSplits;

    if (splitLength < 1) {
      return new long[] {(long) 0};
    }
    long[] readOffsets = new long[numSplits];
    readOffsets[0] = (long) 0;
    for (int split = 1; split < numSplits; split++) {
      readOffsets[split] = splitLength * split;
    }
    return readOffsets;
  }
}
