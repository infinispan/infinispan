package org.infinispan.rest;

import java.io.InputStream;
import java.util.Iterator;
import java.util.stream.Stream;

import org.infinispan.CacheStream;

/**
 * An {@link InputStream} that reads from a {@link CacheStream} of byte[] and produces a JSON output.
 *
 * @since 10.0
 */
public class CacheInputStream extends InputStream {
   private enum State {BEGIN, START_ITEM, ITEM, END_ITEM, SEPARATOR, END, EOF}

   private static final char STREAM_OPEN_CHAR = '[';
   private static final char DATA_ENCLOSING_CHAR = '\"';
   private static final char SEPARATOR = ',';
   private static final char STREAM_CLOSE_CHAR = ']';

   private final Iterator<?> iterator;
   private final Stream<?> stream;
   private final int batchSize;

   private byte[] currentEntry;
   private int cursor = 0;

   private State state = State.BEGIN;

   public CacheInputStream(CacheStream<?> stream, int batchSize) {
      this.batchSize = batchSize;
      this.stream = stream.distributedBatchSize(batchSize);
      this.iterator = stream.iterator();
   }

   @Override
   public int available() {
      return currentEntry == null ? 0 : currentEntry.length - cursor * batchSize;
   }

   @Override
   public synchronized int read() {
      boolean hasNext = iterator.hasNext();
      switch (state) {
         case BEGIN:
            state = hasNext ? State.START_ITEM : State.END;
            return STREAM_OPEN_CHAR;
         case START_ITEM:
            state = State.ITEM;
            return DATA_ENCLOSING_CHAR;
         case END_ITEM:
            state = hasNext ? State.SEPARATOR : State.END;
            return DATA_ENCLOSING_CHAR;
         case SEPARATOR:
            state = State.START_ITEM;
            return SEPARATOR;
         case END:
            state = State.EOF;
            stream.close();
            return STREAM_CLOSE_CHAR;
         case ITEM:
            if (currentEntry == null) {
               if (hasNext) {
                  currentEntry = (byte[]) iterator.next();
               }
            }
            int c = currentEntry == null || cursor == currentEntry.length ? -1 : currentEntry[cursor++] & 0xff;

            if (c != -1) return c;

            state = State.END_ITEM;
            currentEntry = null;
            cursor = 0;
            return read();
         default:
            return -1;
      }
   }
}
