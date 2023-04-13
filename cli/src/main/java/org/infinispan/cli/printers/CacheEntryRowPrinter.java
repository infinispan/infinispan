package org.infinispan.cli.printers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;

/**
 * @since 14.0
 **/
public class CacheEntryRowPrinter implements PrettyRowPrinter {
   private final DateFormat df;
   private final int[] colWidths;

   public CacheEntryRowPrinter(int width, int columns) {
      this.df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      this.colWidths = new int[columns];
      switch (columns) {
         case 1:
            // Key
            colWidths[0] = width;
            break;
         case 2:
            // Key, Value
            if (width <= 20) { // Not enough width, ignore it
               colWidths[0] = 6;
               colWidths[1] = 13;
            } else {
               colWidths[0] = Math.min(width / 3, 15);
               colWidths[1] = width - colWidths[0] - 1;
            }
            break;
         case 7:
            // Key, Value, Metadata
            if (width <= 80) { // Not enough width, ignore it
               colWidths[0] = 6;
               colWidths[1] = 13;
            } else {
               colWidths[0] = Math.min((width - 75) / 3, 15);
               colWidths[1] = width - 75 - colWidths[0];
            }
            colWidths[2] = 6;
            colWidths[3] = 6;
            colWidths[4] = 19;
            colWidths[5] = 19;
            colWidths[6] = 19;
            break;
         default:
            throw new IllegalArgumentException("Illegal number of columns: " + columns);
      }
   }

   @Override
   public boolean showHeader() {
      return true;
   }

   @Override
   public String columnHeader(int column) {
      switch (column) {
         case 0:
            return "Key";
         case 1:
            return "Value";
         case 2:
            return "TTL";
         case 3:
            return "Idle";
         case 4:
            return "Created";
         case 5:
            return "LastUsed";
         case 6:
            return "Expires";
         default:
            throw new IllegalArgumentException();
      }
   }

   @Override
   public int columnWidth(int column) {
      return colWidths[column];
   }

   @Override
   public String formatColumn(int column, String value) {
      if (column < 2) {
         return value; // Key, value: return as-is
      } else {
         long l = Long.parseLong(value);
         if (l < 0) { // Immortal entry
            return "\u221E";
         } else {
            if (column < 4) { // TTL, MaxIdle: return as a duration
               return Duration.ofSeconds(l).toString().substring(2).toLowerCase();
            } else { // Create, last used, expires: return as date/time
               return df.format(l);
            }
         }
      }
   }
}
