package org.infinispan.rest;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;

import org.infinispan.CacheStream;

/**
 * An {@link InputStream} that reads from a {@link CacheStream} of byte[] and produces a JSON output.
 *
 * @since 10.0
 */
public class CacheKeyInputStream extends InputStream {
   private enum State {BEGIN, ITEM, SEPARATOR, END, EOF}

   private static final char STREAM_OPEN_CHAR = '[';
   private static final char SEPARATOR = ',';
   private static final char STREAM_CLOSE_CHAR = ']';

   private final Iterator<?> iterator;
   private final Stream<?> stream;
   private final int batchSize;

   private byte[] currentEntry;
   private int cursor = 0;
   private Boolean hasNext;

   private State state = State.BEGIN;

   public CacheKeyInputStream(CacheStream<?> stream, int batchSize) {
      this.batchSize = batchSize;
      this.stream = stream.distributedBatchSize(batchSize);
      this.iterator = stream.iterator();
      this.hasNext = iterator.hasNext();
   }

   @Override
   public int available() {
      return currentEntry == null ? 0 : currentEntry.length - cursor * batchSize;
   }

   private byte[] escape(byte[] content) {
      String stringified = new String(content, UTF_8);
      String escaped = stringified.replaceAll("\"", "\\\\\"");
      return ("\"" + escaped + "\"").getBytes(UTF_8);
   }

   @Override
   public synchronized int read() {
      for (; ; ) {
         switch (state) {
            case BEGIN:
               state = hasNext ? State.ITEM : State.END;
               return STREAM_OPEN_CHAR;
            case SEPARATOR:
               if (hasNext) {
                  state = State.ITEM;
                  return SEPARATOR;
               }
               state = State.END;
               continue;
            case END:
               state = State.EOF;
               stream.close();
               return STREAM_CLOSE_CHAR;
            case ITEM:
               if (currentEntry == null) {
                  if (hasNext) currentEntry = escape((byte[]) iterator.next());
               }
               int c = currentEntry == null || cursor == currentEntry.length ? -1 : currentEntry[cursor++] & 0xff;

               if (c != -1) return c;

               hasNext = iterator.hasNext();
               cursor = 0;
               currentEntry = null;
               state = State.SEPARATOR;
               continue;
            default:
               return -1;
         }
      }
   }
}
