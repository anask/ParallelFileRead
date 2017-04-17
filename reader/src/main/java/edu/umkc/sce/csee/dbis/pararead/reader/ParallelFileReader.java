package edu.umkc.sce.csee.dbis.pararead.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ParallelFileReader {

  private final String filename;
  private final int numSplits;

  public ParallelFileReader(String filename, int numReadThreads) {
    this.filename = filename;
    this.numSplits = numReadThreads;
  }

  public void readTheFile() {
    // Get the read offset for every thread
    long[] readOffset = getReadOffsets(filename, numSplits);

    // Read the file via threads, each starting at a readOffset
    readInputFile(filename, readOffset);
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

  private void readInputFile(final String filename, long[] readOffsets) {
    final int numOffsets = readOffsets.length;
    int offset = 0;
    while (offset < numOffsets - 1) {
      Thread t = new ReadSplitThread(filename, readOffsets[offset], readOffsets[offset + 1]);
      t.start();
      offset++;
    }
  }

  private class ReadSplitThread extends Thread {
    private final String filename;
    private final long start;
    private final long end;

    public ReadSplitThread(String filename, long start, long end) {
      this.filename = filename;
      this.start = start;
      this.end = end;
    }

    @Override
    public void run() {
      readFileAtTo();
    }

    private void readFileAtTo() {
      try {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        raf.seek(start);
        while (raf.getFilePointer() < end) {
          String data = raf.readLine();
          System.out.println(data);
        }
        raf.close();

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

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
