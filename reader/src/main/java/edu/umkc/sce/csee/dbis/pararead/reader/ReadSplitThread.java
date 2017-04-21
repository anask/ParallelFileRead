package edu.umkc.sce.csee.dbis.pararead.reader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

public class ReadSplitThread extends Thread {
  private final String filename;
  private final long start;
  private final long end;
  private final int[] counter;

  public ReadSplitThread(String filename, long start, long end, int[] counters) {
    this.filename = filename;
    this.start = start;
    this.end = end;
    this.counter = counters;

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
        Random rand = new Random();
        int cell = Math.abs(rand.nextInt() % counter.length);
        this.counter[cell]++;
      }
      raf.close();

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
