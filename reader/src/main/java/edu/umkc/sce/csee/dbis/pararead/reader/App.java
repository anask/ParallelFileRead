package edu.umkc.sce.csee.dbis.pararead.reader;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

/**
 * Anas Katib
 * Code to create an input file and read it in parallel fashion.
 *
 */
public class App {
  public static void main(String[] args) {
    String filename = "input.txt";
    // 100 lines each with line_number + 10 chars
    createInputFile(100,10,filename);
    int numReadThreads = 4;
    int [] numLinesCounters = new int [numReadThreads];
    
    ParallelFileReader pfr = new ParallelFileReader(filename, numReadThreads);
    System.out.println(Arrays.toString(numLinesCounters));
    pfr.readTheFile(numLinesCounters);
    System.out.println(Arrays.toString(numLinesCounters));
    int sum = 0;
    for (int c = 0; c < numLinesCounters.length; c++){
      sum += numLinesCounters[c];
    }
    System.out.println("Read: "+sum+" lines.");
  }

  private static void createInputFile(int numLines, int lineWidth, String fileName) {
    try {
      PrintWriter writer = new PrintWriter(fileName, "UTF-8");
      int id = 0;
      for (int i = 0; i < numLines; i++) {
        if (id == 10)
          id = 0;
        String line = new String(new char[lineWidth]).replace("\0", String.valueOf(id++));
        writer.println(String.valueOf(i) + "_" + line);
      }
      writer.close();
    } catch (IOException e) {
      System.err.println("What the heck!?");
    }

  }
}
