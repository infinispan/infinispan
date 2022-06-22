package org.infinispan.cli.printers;

import java.io.IOException;
import java.util.Map;

import org.aesh.command.shell.Shell;

/**
 * @since 14.0
 **/
public class TablePrettyPrinter implements PrettyPrinter {
   private final Shell shell;
   private final PrettyRowPrinter rowPrinter;
   private boolean header = true;

   public TablePrettyPrinter(Shell shell, PrettyRowPrinter rowPrinter) {
      this.shell = shell;
      this.rowPrinter = rowPrinter;
   }

   @Override
   public void printItem(Map<String, String> item) {
      int cols = item.size();
      if (rowPrinter.showHeader() && header) {
         for(int row = 0; row < 2; row++) {
            for (int col = 0; col < cols; col++) {
               if (col > 0) {
                  shell.write(row == 0 ? "|" : "+");
               }
               if (row == 0) {
                  String format = "%-" + rowPrinter.columnWidth(col) + "s";
                  shell.write(String.format(format, rowPrinter.columnHeader(col)));
               } else {
                  shell.write("-".repeat(rowPrinter.columnWidth(col)));
               }
            }
            shell.writeln("");
         }
         header = false;
      }

      int[] colsWrap = new int[cols];
      int remaining = cols;
      do {
         int i = 0;
         for (Map.Entry<String, String> col : item.entrySet()) {
            if (i > 0) {
               shell.write("|");
            }
            String v = rowPrinter.formatColumn(i, col.getValue());
            int width = rowPrinter.columnWidth(i);
            String format = "%-" + width + "s";
            if (i < 4) {
               if (colsWrap[i] < 0) {
                  // We've already printed the whole value
                  v = "";
               } else if (v.length() - colsWrap[i] > width) {
                  // Just print characters that fit skipping any that we've already printed
                  v = v.substring(colsWrap[i], colsWrap[i] + width);
                  colsWrap[i] += width;
               } else {
                  // The rest of the value fits
                  v = v.substring(colsWrap[i]);
                  colsWrap[i] = -1;
                  remaining--;
               }
               shell.write(String.format(format, v));
            } else {
               if (colsWrap[i] == 0) {
                  shell.write(String.format(format, v));
                  colsWrap[i] = -1;
                  remaining--;
               } else {
                  shell.write(" ".repeat(width));
               }
            }
            i++;
         }
         shell.writeln("");
      } while (remaining > 0);

   }

   @Override
   public void close() throws IOException {
   }
}
