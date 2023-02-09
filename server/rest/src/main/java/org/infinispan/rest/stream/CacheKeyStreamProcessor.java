package org.infinispan.rest.stream;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.reactivestreams.Publisher;

/**
 * A {@link CacheChunkedStream} that reads from a <code>byte[]</code> and produces a JSON output.
 *
 * @since 10.0
 */
public class CacheKeyStreamProcessor extends CacheChunkedStream<byte[]> {
   private enum State {BEGIN, ITEM, SEPARATOR, END, EOF}

   private static final byte STREAM_OPEN_CHAR = '[';
   private static final byte SEPARATOR = ',';
   private static final byte STREAM_CLOSE_CHAR = ']';

   private byte[] currentEntry;
   private int cursor = 0;
   private State state = State.BEGIN;
   private volatile boolean elementConsumed = true;

   public CacheKeyStreamProcessor(Publisher<byte[]> publisher) {
      super(publisher);
   }

   private byte[] escape(byte[] content) {
      if (content == null) return null;
      String stringified = new String(content, UTF_8);
      String escaped = stringified.replaceAll("\"", "\\\\\"");
      return ("\"" + escaped + "\"").getBytes(UTF_8);
   }

   @Override
   public void setCurrent(byte[] value) {
      currentEntry = escape(value);
      elementConsumed = false;
   }

   @Override
   public boolean hasElement() {
      return !elementConsumed;
   }

   @Override
   public boolean isEndOfInput() {
      return state == State.EOF;
   }

   @Override
   public byte read() {
      if (state == null) state = State.BEGIN;
      for (; ; ) {
         switch (state) {
            case BEGIN:
               state = currentEntry != null ? State.ITEM : State.END;
               return STREAM_OPEN_CHAR;
            case SEPARATOR:
               if (currentEntry != null) {
                  state = State.ITEM;
                  return SEPARATOR;
               }
               state = State.END;
               continue;
            case END:
               state = State.EOF;
               elementConsumed = true;
               return STREAM_CLOSE_CHAR;
            case ITEM:
               int c = currentEntry == null || cursor == currentEntry.length ? -1 : currentEntry[cursor++] & 0xff;
               if (c != -1) {
                  return (byte) c;
               }

               cursor = 0;
               currentEntry = null;
               state = State.SEPARATOR;
               elementConsumed = true;
            default:
               return -1;
         }
      }
   }
}
