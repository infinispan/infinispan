package org.infinispan.cli.printers;

import java.io.IOException;
import java.util.Map;

import org.aesh.command.shell.Shell;

/**
 * @since 14.0
 **/
public class CsvPrettyPrinter extends AbstractPrettyPrinter {
   private final PrettyRowPrinter rowPrinter;
   private boolean header = true;

   protected CsvPrettyPrinter(Shell shell, PrettyRowPrinter rowPrinter) {
      super(shell);
      this.rowPrinter = rowPrinter;
   }

   @Override
   public void printItem(Map<String, String> item) {
      if (header) {
         shell.write("# ");
         boolean comma = false;
         for (int i = 0; i < item.size(); i++) {
            if (comma) {
               shell.write(",");
            } else {
               comma = true;
            }
            shell.write(rowPrinter.columnHeader(i));
         }
         shell.writeln("");
         header = false;
      }
      boolean comma = false;
      for (Map.Entry<String, String> column : item.entrySet()) {
         if (comma) {
            shell.write(",");
         } else {
            comma = true;
         }
         shell.write('"');
         shell.write(column.getValue().replace("\"","\\\""));
         shell.write('"');
      }
      shell.writeln("");
   }

   @Override
   public void close() throws IOException {
   }
}
