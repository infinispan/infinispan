package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheEntryMetadata;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.cache.CacheEntryImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryMetadataImpl;
import org.infinispan.hotrod.impl.cache.CacheEntryVersionImpl;
import org.infinispan.hotrod.impl.iteration.KeyTracker;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @since 14.0
 */
public class IterationNextOperation<K, E> extends HotRodOperation<IterationNextResponse<K, E>> {

   private final byte[] iterationId;
   private final Channel channel;
   private final KeyTracker segmentKeyTracker;

   private byte[] finishedSegments;
   private int entriesSize = -1;
   private List<CacheEntry<K, E>> entries;
   private int projectionsSize;
   private int untrackedEntries;

   protected IterationNextOperation(OperationContext operationContext, CacheOptions options, byte[] iterationId, Channel channel,
                                    KeyTracker segmentKeyTracker, DataFormat dataFormat) {
      super(operationContext, ITERATION_NEXT_REQUEST, ITERATION_NEXT_RESPONSE, options, dataFormat);
      this.iterationId = iterationId;
      this.channel = channel;
      this.segmentKeyTracker = segmentKeyTracker;
   }

   @Override
   public CompletableFuture<IterationNextResponse<K, E>> execute() {
      if (!channel.isActive()) {
         throw HOTROD.channelInactive(channel.remoteAddress(), channel.remoteAddress());
      }
      scheduleRead(channel);
      sendArrayOperation(channel, iterationId);
      return this;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (entriesSize < 0) {
         finishedSegments = ByteBufUtil.readArray(buf);
         entriesSize = ByteBufUtil.readVInt(buf);
         if (entriesSize == 0) {
            IntSet finishedSegmentSet = IntSets.from(finishedSegments);
            segmentKeyTracker.segmentsFinished(finishedSegmentSet);
            complete(new IterationNextResponse(status, Collections.emptyList(), finishedSegmentSet, false));
            return;
         }
         entries = new ArrayList<>(entriesSize);
         projectionsSize = operationContext.getCodec().readProjectionSize(buf);
         decoder.checkpoint();
      }
      while (entries.size() + untrackedEntries < entriesSize) {
         short meta = operationContext.getCodec().readMeta(buf);
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         CacheEntryMetadata metadata;
         if (meta == 1) {
            short flags = buf.readUnsignedByte();
            if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
               creation = buf.readLong();
               lifespan = ByteBufUtil.readVInt(buf);
            }
            if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
               lastUsed = buf.readLong();
               maxIdle = ByteBufUtil.readVInt(buf);
            }
            CacheEntryExpiration expiration;
            if (lifespan < 0) {
               if (maxIdle < 0) {
                  expiration = CacheEntryExpiration.IMMORTAL;
               } else {
                  expiration = CacheEntryExpiration.withMaxIdle(Duration.ofSeconds(maxIdle));
               }
            } else {
               if (maxIdle < 0) {
                  expiration = CacheEntryExpiration.withLifespan(Duration.ofSeconds(lifespan));
               } else {
                  expiration = CacheEntryExpiration.withLifespanAndMaxIdle(Duration.ofSeconds(lifespan), Duration.ofSeconds(maxIdle));
               }
            }
            metadata = new CacheEntryMetadataImpl(creation, lastUsed, expiration, new CacheEntryVersionImpl(buf.readLong()));
         } else {
            metadata = new CacheEntryMetadataImpl();
         }
         byte[] key = ByteBufUtil.readArray(buf);
         E value;
         if (projectionsSize > 1) {
            Object[] projections = new Object[projectionsSize];
            for (int j = 0; j < projectionsSize; j++) {
               projections[j] = unmarshallValue(ByteBufUtil.readArray(buf));
            }
            value = (E) projections;
         } else {
            value = unmarshallValue(ByteBufUtil.readArray(buf));
         }

         if (segmentKeyTracker.track(key, status, operationContext.getConfiguration().getClassAllowList())) {
            K unmarshallKey = dataFormat().keyToObj(key, operationContext.getConfiguration().getClassAllowList());
            entries.add(new CacheEntryImpl<>(unmarshallKey, value, metadata));
         } else {
            untrackedEntries++;
         }
         decoder.checkpoint();
      }
      IntSet finishedSegmentSet = IntSets.from(finishedSegments);
      segmentKeyTracker.segmentsFinished(finishedSegmentSet);
      if (HotRodConstants.isInvalidIteration(status)) {
         throw HOTROD.errorRetrievingNext(new String(iterationId, HOTROD_STRING_CHARSET));
      }
      complete(new IterationNextResponse<>(status, entries, finishedSegmentSet, entriesSize > 0));
   }

   private <M> M unmarshallValue(byte[] bytes) {
      return dataFormat().valueToObj(bytes, operationContext.getConfiguration().getClassAllowList());
   }
}
