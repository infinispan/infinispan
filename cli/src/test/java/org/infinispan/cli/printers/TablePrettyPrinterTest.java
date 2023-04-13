package org.infinispan.cli.printers;


import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;

import org.infinispan.cli.AeshTestShell;
import org.infinispan.cli.util.JsonReaderIterable;
import org.junit.Test;

/**
 * @since 15.0
 **/
public class TablePrettyPrinterTest {

   public static final int WIDTH = 120;
   public static final int COLUMNS = 7;

   @Test
   public void testTableWrapping() throws IOException {

      CacheEntryRowPrinter rowPrinter = new CacheEntryRowPrinter(WIDTH, COLUMNS);
      AeshTestShell shell = new AeshTestShell();
      TablePrettyPrinter t = new TablePrettyPrinter(shell, rowPrinter);
      try (InputStream is = TablePrettyPrinter.class.getResourceAsStream("/printers/entries.json")) {
         Iterator<Map<String, String>> it = new JsonReaderIterable(is).iterator();
         t.printItem(it.next());
         checkRow(rowPrinter, shell.getBuffer(), 17);
         shell.clear();
         t.printItem(it.next());
         checkRow(rowPrinter, shell.getBuffer(), 15);
      }
   }

   private static void checkRow(CacheEntryRowPrinter rowPrinter, String row, int numLines) {
      String[] lines = row.split("\n");
      // Ensure we have the right number of lines
      assertEquals(numLines, lines.length);
      for (String line : lines) {
         // Ensure the lines fit the width
         assertEquals(WIDTH, line.length());
         int pos = -1;
         char separator = line.startsWith("---") ? '+' : '|';
         // Ensure the column separators are in the right place
         for (int i = 0; i < COLUMNS - 1; i++) {
            pos += rowPrinter.columnWidth(i) + 1;
            assertEquals(line + ":" + pos, separator, line.charAt(pos));
         }
      }
   }
}
