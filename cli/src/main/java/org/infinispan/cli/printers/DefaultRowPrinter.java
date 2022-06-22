package org.infinispan.cli.printers;

/**
 * @since 14.0
 **/
public class DefaultRowPrinter implements PrettyRowPrinter {
   private final int[] colWidths;

   public DefaultRowPrinter(int width, int columns) {
      this.colWidths = new int[columns];
      int effectiveWidth = width - columns + 1;
      int columnWidth = effectiveWidth / columns;
      for (int i = 0; i < columns - 1; i++) {
         colWidths[i] = columnWidth;
         effectiveWidth -= columnWidth;
      }
      // The last column gets the remaining width
      colWidths[columns - 1] = effectiveWidth;
   }

   @Override
   public boolean showHeader() {
      return false;
   }

   @Override
   public String columnHeader(int column) {
      return ""; // TODO
   }

   @Override
   public int columnWidth(int column) {
      return colWidths[column];
   }

   @Override
   public String formatColumn(int column, String value) {
      return value;
   }
}
