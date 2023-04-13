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
         for (int row = 0; row < 2; row++) {
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
               int offset = colsWrap[i];
               if (offset < 0) {
                  // We've already printed the whole value
                  v = "";
               } else {
                  int lf = v.indexOf("\n", offset) - offset;
                  if (lf < 0 || lf > width) {
                     // No LFs inside the range
                     if (v.length() - offset <= width) {
                        // The rest of the value fits
                        v = v.substring(offset);
                        colsWrap[i] = -1;
                        remaining--;
                     } else {
                        // Just print characters that fit skipping any that we've already printed
                        v = v.substring(offset, offset + width);
                        colsWrap[i] += width;
                     }
                  } else {
                     // LF inside the range, just print up to it
                     v = v.substring(offset, offset + lf);
                     colsWrap[i] += lf + 1;
                  }
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
