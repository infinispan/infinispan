package org.infinispan.rest.stream;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.metadata.Metadata;
import org.reactivestreams.Publisher;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;

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

   private static final byte[] KEY_LABEL = "\"key\":".getBytes();
   private static final byte[] VALUE_LABEL = "\"value\":".getBytes();

   private static final byte OPEN_ITEM_CHAR = '{';
   private static final byte SEPARATOR = ',';
   private static final byte CLOSE_ITEM_CHAR = '}';
   private final boolean includeMetadata;

   public CacheEntryStreamProcessor(Publisher<CacheEntry<?, ?>> publisher, boolean keysAreJson, boolean valuesAreJson,
         boolean includeMetadata) {
      super(publisher);
      this.keysAreJson = keysAreJson;
      this.valuesAreJson = valuesAreJson;
      this.includeMetadata = includeMetadata;
   }

   @Override
   public void subscribe(ChannelHandlerContext ctx) {
      publisher.subscribe(new EntrySubscriber(ctx, ctx.alloc()));
   }

   class EntrySubscriber extends ByteBufSubscriber<CacheEntry<?, ?>> {

      protected EntrySubscriber(ChannelHandlerContext ctx, ByteBufAllocator allocator) {
         super(ctx, allocator);
      }

      @Override
      void writeItem(CacheEntry<?, ?> item, ByteBuf pending) {
         byte[] keyBytes = escape(item.getKey(), keysAreJson);
         byte[] valueBytes = escape(item.getValue(), valuesAreJson);
         byte[] metadataBytes = includeMetadata ? metadataBytes(item) : null;

         // 1 for {
         // Key label
         // Key bytes
         // 2 for "" around key if not json
         // 1 for ,
         // Value label
         // Value bytes
         // 2 for "" around key if not json
         // 1 for }
         int bytesRequired = 1 + KEY_LABEL.length + keyBytes.length + (keysAreJson ? 0 : 2)
               + 1 + VALUE_LABEL.length + valueBytes.length + (valuesAreJson ? 0 : 2) + 1;
         // Also add in metadataBytes as necessary
         if (metadataBytes != null) {
            bytesRequired += 1 + metadataBytes.length;
         }

         pending.ensureWritable(bytesRequired);

         pending.writeByte(OPEN_ITEM_CHAR);
         pending.writeBytes(KEY_LABEL);
         if (!keysAreJson) pending.writeByte('"');
         pending.writeBytes(keyBytes);
         if (!keysAreJson) pending.writeByte('"');
         pending.writeByte(SEPARATOR);
         pending.writeBytes(VALUE_LABEL);
         if (!valuesAreJson) pending.writeByte('"');
         pending.writeBytes(valueBytes);
         if (!valuesAreJson) pending.writeByte('"');
         if (metadataBytes != null) {
            pending.writeByte(SEPARATOR);
            pending.writeBytes(metadataBytes);
         }
         pending.writeByte(CLOSE_ITEM_CHAR);
      }
   }

   private byte[] escape(Object content, boolean json) {
      byte[] asUTF = readContentAsBytes(content);

      if (json) return asUTF;

      return Json.help.escape(new String(asUTF)).getBytes(StandardCharsets.UTF_8);
   }

   private static byte[] metadataBytes(CacheEntry<?, ?> currentEntry) {
      if (!(currentEntry instanceof InternalCacheEntry)) {
         return null;
      }
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

      Metadata metadata = currentEntry.getMetadata();
      EntryVersion version = (metadata == null) ? null : metadata.version();
      return metadataBytes(lifespanInSeconds, maxIdleInSeconds, created, lastUsed, expiryTime, version);
   }

   private static byte[] metadataBytes(long timeToLiveSeconds, long maxIdleTimeSeconds, long created, long lastUsed,
                                       long expireTime, EntryVersion entryVersion) {
      Long version = null;
      Integer topologyId = null;

      if (entryVersion instanceof NumericVersion) {
         version = ((NumericVersion) entryVersion).getVersion();
      } else if (entryVersion instanceof SimpleClusteredVersion) {
         version = ((SimpleClusteredVersion) entryVersion).getVersion();
         topologyId = ((SimpleClusteredVersion) entryVersion).getTopologyId();
      }

      StringBuilder metadata = new StringBuilder();
      metadata.append("\"timeToLiveSeconds\":");
      metadata.append(timeToLiveSeconds);
      metadata.append(",\"maxIdleTimeSeconds\":");
      metadata.append(maxIdleTimeSeconds);
      metadata.append(",\"created\":");
      metadata.append(created);
      metadata.append(",\"lastUsed\":");
      metadata.append(lastUsed);
      metadata.append(",\"expireTime\":");
      metadata.append(expireTime);

      if (version != null) {
         metadata.append(",\"version\":");
         metadata.append(version);
      }
      if (topologyId != null) {
         metadata.append(",\"topologyId\":");
         metadata.append(topologyId);
      }

      return metadata.toString().getBytes();
   }
}
