package org.infinispan.cli.printers;

/**
 * @since 14.0
 **/
public interface PrettyRowPrinter {

   boolean showHeader();

   String columnHeader(int column);

   int columnWidth(int column);

   String formatColumn(int column, String value);
}
