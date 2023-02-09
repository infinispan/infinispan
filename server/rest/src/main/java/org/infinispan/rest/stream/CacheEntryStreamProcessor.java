package org.infinispan.rest.stream;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.reactivestreams.Publisher;

/**
 * A {@link CacheChunkedStream} that reads {@link Map.Entry} and produces a JSON output.
 * For example:
 * <p>
 * <pre>{@code
 * [
 *   {"key":1,"value":"value","timeToLiveSeconds": -1, "maxIdleTimeSeconds": -1, "created": -1, "lastUsed": -1, "expireTime": -1},
 *   {"key":2,"value":"value2","timeToLiveSeconds": -1, "maxIdleTimeSeconds": -1, "created": -1, "lastUsed": -1, "expireTime": -1}
 * ]
 * }</pre>
 *
 *
 * @since 12.0
 */
public class CacheEntryStreamProcessor extends CacheChunkedStream<CacheEntry<?, ?>> {
   private final boolean keysAreJson;
   private final boolean valuesAreJson;

   private enum State {BEGIN, BEGIN_ITEM, NEXT_ITEM, ITEM_KEY, ITEM_VALUE, ITEM_METADATA, SEPARATOR_KEY, SEPARATOR_VALUE, END_ITEM, SEPARATOR, END, EOF}

   private static final byte[] KEY_LABEL = "\"key\":".getBytes();
   private static final byte[] VALUE_LABEL = "\"value\":".getBytes();

   private static final byte OPEN_CHAR = '[';
   private static final byte OPEN_ITEM_CHAR = '{';
   private static final byte SEPARATOR = ',';
   private static final byte CLOSE_ITEM_CHAR = '}';
   private static final byte CLOSE_CHAR = ']';
   private final boolean includeMetadata;

   private Map.Entry<?, ?> currentEntry;
   private byte[] currentKey;
   private byte[] currentValue;
   private byte[] currentMetadata;
   private int keyCursor = 0;
   private int valueCursor = 0;
   private int mdCursor = 0;
   private int keyLabelCursor = 0;
   private int valueLabelCursor = 0;
   private State state = State.BEGIN;
   private volatile boolean elementConsumed = true;

   static class Metadata {
      public static final byte[] EMPTY = new Metadata().bytes();
      private final long timeToLiveSeconds;
      private final long maxIdleTimeSeconds;
      private final long created;
      private final long lastUsed;
      private final long expireTime;

      public Metadata() {
         timeToLiveSeconds = -1;
         maxIdleTimeSeconds = -1;
         created = -1;
         lastUsed = -1;
         expireTime = -1;
      }

      public Metadata(long timeToLiveSeconds, long maxIdleTimeSeconds, long created, long lastUsed, long expireTime) {
         this.timeToLiveSeconds = timeToLiveSeconds;
         this.maxIdleTimeSeconds = maxIdleTimeSeconds;
         this.created = created;
         this.lastUsed = lastUsed;
         this.expireTime = expireTime;
      }

      public byte[] bytes() {
         return ("\"timeToLiveSeconds\":" + timeToLiveSeconds + ","
               + "\"maxIdleTimeSeconds\":" + maxIdleTimeSeconds + ","
               + "\"created\":" + created + ","
               + "\"lastUsed\":" + lastUsed + ","
               + "\"expireTime\":" + expireTime)
               .getBytes();
      }
   }

   public CacheEntryStreamProcessor(Publisher<CacheEntry<?, ?>> publisher, boolean keysAreJson, boolean valuesAreJson,
                                    boolean includeMetadata) {
      super(publisher);
      this.keysAreJson = keysAreJson;
      this.valuesAreJson = valuesAreJson;
      this.includeMetadata = includeMetadata;
   }

   private byte[] escape(Object content, boolean json) {
      byte[] asUTF = readContentAsBytes(content);

      if (json) return asUTF;

      String escape = "\"" + Json.help.escape(new String(asUTF)) + "\"";
      return escape.getBytes(StandardCharsets.UTF_8);
   }

   @Override
   public boolean isEndOfInput() {
      return state == State.EOF;
   }

   @Override
   public void setCurrent(CacheEntry<?, ?> value) {
      currentEntry = value;
      if (currentEntry == null) {
         currentKey = null;
         currentValue = null;
      } else {
         currentKey = escape(currentEntry.getKey(), keysAreJson);
         currentValue = escape(currentEntry.getValue(), valuesAreJson);
      }

      if (includeMetadata) {
         loadMetadata();
      }
      elementConsumed = false;
   }

   @Override
   public boolean hasElement() {
      return !elementConsumed;
   }

   @Override
   public byte read() {
      if (state == null) state = State.BEGIN;
      for (; ; ) {
         switch (state) {
            case BEGIN:
               state = currentEntry != null ? State.BEGIN_ITEM : State.END;
               return OPEN_CHAR;
            case BEGIN_ITEM:
               state = State.ITEM_KEY;
               return OPEN_ITEM_CHAR;
            case NEXT_ITEM:
               state = State.BEGIN_ITEM;
               return SEPARATOR;
            case END_ITEM:
               state = currentEntry != null ? State.NEXT_ITEM : State.END;
               return CLOSE_ITEM_CHAR;
            case SEPARATOR_KEY:
               state = State.ITEM_VALUE;
               return SEPARATOR;
            case SEPARATOR_VALUE:
               state = State.ITEM_METADATA;
               return SEPARATOR;
            case SEPARATOR:
               if (currentEntry != null) {
                  state = State.ITEM_KEY;
                  return SEPARATOR;
               }
               state = State.END;
               continue;
            case END:
               state = State.EOF;
               elementConsumed = true;
               return CLOSE_CHAR;
            case ITEM_KEY:
               if (keyLabelCursor < KEY_LABEL.length) {
                  int c = KEY_LABEL[keyLabelCursor++] & 0xff;
                  return (byte) c;
               }

               byte[] key = currentKey;

               int ck = currentEntry == null || keyCursor == key.length ? -1 : key[keyCursor++] & 0xff;
               if (ck != -1) {
                  return (byte) ck;
               }
               keyCursor = 0;
               keyLabelCursor = 0;
               state = State.SEPARATOR_KEY;
               continue;
            case ITEM_VALUE:
               if (valueLabelCursor < VALUE_LABEL.length) {
                  int c = VALUE_LABEL[valueLabelCursor++] & 0xff;
                  return (byte) c;
               }

               int cv = valueCursor == currentValue.length ? -1 : currentValue[valueCursor++] & 0xff;
               if (cv != -1) {
                  return (byte) cv;
               }
               valueCursor = 0;
               valueLabelCursor = 0;
               if (includeMetadata) {
                  state = State.SEPARATOR_VALUE;
               } else {
                  endItem();
                  return -1;
               }
               continue;
            case ITEM_METADATA:
               int cm = mdCursor == currentMetadata.length ? -1 : currentMetadata[mdCursor++] & 0xff;
               if (cm != -1) {
                  return (byte) cm;
               }

               endItem();
               mdCursor = 0;
               currentMetadata = null;
            default:
               return -1;
         }
      }
   }

   private void endItem() {
      currentEntry = null;
      currentKey = null;
      currentValue = null;
      state = State.END_ITEM;
      elementConsumed = true;
   }

   private void loadMetadata() {
      if (currentEntry instanceof InternalCacheEntry) {
         InternalCacheEntry ice = (InternalCacheEntry) currentEntry;
         // add metadata
         long lifespanInSeconds = ice.getLifespan();
         if (lifespanInSeconds > -1) {
            lifespanInSeconds = TimeUnit.MILLISECONDS.toSeconds(lifespanInSeconds);
         }
         long maxIdleInSeconds = ice.getMaxIdle();
         if (maxIdleInSeconds > -1) {
            maxIdleInSeconds = TimeUnit.MILLISECONDS.toSeconds(maxIdleInSeconds);
         }
         long created = ice.getCreated();
         long lastUsed = ice.getLastUsed();
         long expiryTime = ice.getExpiryTime();
         currentMetadata = new Metadata(lifespanInSeconds, maxIdleInSeconds, created, lastUsed, expiryTime).bytes();
      } else {
         currentMetadata = Metadata.EMPTY;
      }
   }
}
