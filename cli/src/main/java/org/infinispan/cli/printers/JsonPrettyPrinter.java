package org.infinispan.cli.printers;

import java.io.IOException;
import java.util.Map;

import org.aesh.command.shell.Shell;

/**
 * @since 14.0
 **/
public class JsonPrettyPrinter extends AbstractPrettyPrinter {
   private boolean commaRow;

   protected JsonPrettyPrinter(Shell shell) {
      super(shell);
      shell.writeln("[");
   }

   @Override
   public void printItem(Map<String, String> item) {
      if (commaRow) {
         shell.writeln(",");
      } else {
         commaRow = true;
      }
      boolean simple = item.size() == 1;
      if (!simple) {
         shell.write("{");
      }
      boolean commaCol = false;
      for (Map.Entry<String, String> column : item.entrySet()) {
         if (commaCol) {
            shell.writeln(", ");
         } else {
            commaCol = true;
         }
         if (!simple) {
            shell.write(column.getKey());
            shell.write(": ");
         }
         shell.write('"');
         shell.write(column.getValue());
         shell.write('"');
      }
      if (!simple) {
         shell.write("}");
      }
   }

   @Override
   public void close() throws IOException {
      shell.writeln("");
      shell.writeln("]");
   }
}
