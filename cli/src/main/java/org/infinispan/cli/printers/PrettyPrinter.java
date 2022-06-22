package org.infinispan.cli.printers;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;

import org.aesh.command.shell.Shell;

/**
 * @since 14.0
 **/
public interface PrettyPrinter extends Closeable {

   enum PrettyPrintMode {
      TABLE,
      JSON,
      CSV
   }

   static PrettyPrinter forMode(PrettyPrintMode mode, Shell shell, PrettyRowPrinter rowPrinter) {
      switch (mode) {
         case TABLE:
            return new TablePrettyPrinter(shell, rowPrinter);
         case JSON:
            return new JsonPrettyPrinter(shell);
         case CSV:
            return new CsvPrettyPrinter(shell, rowPrinter);
         default:
            throw new IllegalArgumentException(mode.name());
      }
   }

   void printItem(Map<String, String> item);

   default void print(Iterator<String> it) {
      it.forEachRemaining(i -> printItem(Map.of("", i)));
   }

   default void print(Iterable<Map<String, String>> it) {
      it.forEach(this::printItem);
   }
}
