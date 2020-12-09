package org.infinispan.rest;

import org.infinispan.CacheStream;
import org.infinispan.container.entries.InternalCacheEntry;

import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * An {@link InputStream} that reads from a {@link CacheStream} of byte[] and produces a JSON output.
 * For example:
 * <p>
 * [{"key":1,"value":"value","timeToLiveSeconds": -1, "maxIdleTimeSeconds": -1, "created": -1, "lastUsed": -1, "expireTime": -1},
 * {"key":2,"value":"value2","timeToLiveSeconds": -1, "maxIdleTimeSeconds": -1, "created": -1, "lastUsed": -1, "expireTime": -1}]
 *
 * @since 12.0
 */
public class CacheEntryInputStream extends InputStream {
   private enum State {BEGIN, BEGIN_ITEM, NEXT_ITEM, ITEM_KEY, ITEM_VALUE, ITEM_METADATA, SEPARATOR_KEY, SEPARATOR_VALUE, END_ITEM, SEPARATOR, END, EOF}

   private static final byte[] KEY_LABEL = "\"key\":".getBytes();
   private static final byte[] VALUE_LABEL = "\"value\":".getBytes();

   private static final char OPEN_CHAR = '[';
   private static final char OPEN_ITEM_CHAR = '{';
   private static final char SEPARATOR = ',';
   private static final char CLOSE_ITEM_CHAR = '}';
   private static final char CLOSE_CHAR = ']';

   private final Iterator<? extends Map.Entry<?, ?>> iterator;
   private final Stream<? extends Map.Entry<?, ?>> stream;
   private final int batchSize;
   private boolean includeMetadata;

   private Map.Entry<?, ?> currentEntry;
   private byte[] currentMetadata;
   private int cursor = 0;
   private int keyCursor = 0;
   private int valueCursor = 0;
   private int mdCursor = 0;
   private int keyLabelCursor = 0;
   private int valueLabelCursor = 0;
   private Boolean hasNext;

   private State state = State.BEGIN;

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
         return ("\"timeToLiveSeconds\": " + timeToLiveSeconds + ", "
               + "\"maxIdleTimeSeconds\": " + maxIdleTimeSeconds + ", "
               + "\"created\": " + created + ", "
               + "\"lastUsed\": " + lastUsed + ", "
               + "\"expireTime\": " + expireTime)
               .getBytes();
      }
   }

   public CacheEntryInputStream(CacheStream<? extends Map.Entry<?, ?>> stream,
                                int batchSize,
                                boolean includeMetadata) {
      this.stream = stream.distributedBatchSize(batchSize);
      this.iterator = stream.iterator();
      this.hasNext = iterator.hasNext();
      this.includeMetadata = includeMetadata;
      this.batchSize = batchSize;
   }

   @Override
   public int available() {
      if (currentEntry == null) {
         return 0;
      }

      int keySize = ((byte[]) currentEntry.getKey()).length;
      int valueSize = ((byte[]) currentEntry.getValue()).length;
      int metadataSize = includeMetadata ? currentMetadata.length : 0;
      return keySize + valueSize + metadataSize - cursor * batchSize;
   }

   @Override
   public synchronized int read() {
      for (; ; ) {
         switch (state) {
            case BEGIN:
               state = hasNext ? State.BEGIN_ITEM : State.END;
               return OPEN_CHAR;
            case BEGIN_ITEM:
               state = State.ITEM_KEY;
               return OPEN_ITEM_CHAR;
            case NEXT_ITEM:
               state = State.BEGIN_ITEM;
               return SEPARATOR;
            case END_ITEM:
               state = hasNext ? State.NEXT_ITEM : State.END;
               return CLOSE_ITEM_CHAR;
            case SEPARATOR_KEY:
               state = State.ITEM_VALUE;
               return SEPARATOR;
            case SEPARATOR_VALUE:
               state = State.ITEM_METADATA;
               return SEPARATOR;
            case SEPARATOR:
               if (hasNext) {
                  state = State.ITEM_KEY;
                  return SEPARATOR;
               }
               state = State.END;
               continue;
            case END:
               state = State.EOF;
               stream.close();
               return CLOSE_CHAR;
            case ITEM_KEY:
               if (currentEntry == null) {
                  if (hasNext) {
                     currentEntry = iterator.next();
                     if (includeMetadata) {
                        loadMetadata();
                     }
                  }
               }

               if (keyLabelCursor < KEY_LABEL.length) {
                  return KEY_LABEL[keyLabelCursor++] & 0xff;
               }

               byte[] key = (byte[]) currentEntry.getKey();

               int ck = currentEntry == null || keyCursor == key.length ? -1 : key[keyCursor++] & 0xff;
               cursor++;
               if (ck != -1)
                  return ck;
               keyCursor = 0;
               keyLabelCursor = 0;
               state = State.SEPARATOR_KEY;
               continue;
            case ITEM_VALUE:
               if (valueLabelCursor < VALUE_LABEL.length) {
                  return VALUE_LABEL[valueLabelCursor++] & 0xff;
               }

               byte[] value = (byte[]) currentEntry.getValue();
               int cv = valueCursor == value.length ? -1 : value[valueCursor++] & 0xff;
               cursor++;
               if (cv != -1)
                  return cv;
               valueCursor = 0;
               valueLabelCursor = 0;
               if (includeMetadata) {
                  state = State.SEPARATOR_VALUE;
               } else {
                  endItem();
               }
               continue;
            case ITEM_METADATA:
               int cm = mdCursor == currentMetadata.length ? -1 : currentMetadata[mdCursor++] & 0xff;
               cursor++;
               if (cm != -1)
                  return cm;

               endItem();
               mdCursor = 0;
               currentMetadata = null;
               continue;
            default:
               return -1;
         }
      }
   }

   private void endItem() {
      currentEntry = null;
      state = State.END_ITEM;
      hasNext = iterator.hasNext();
      cursor = 0;
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
