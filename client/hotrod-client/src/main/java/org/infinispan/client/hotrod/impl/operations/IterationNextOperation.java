package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextOperation<K, E> extends AbstractCacheOperation<IterationNextResponse<K, E>> {

   private final byte[] iterationId;
   private final KeyTracker segmentKeyTracker;

   private byte[] finishedSegments;
   private int entriesSize = -1;
   private List<Entry<K, E>> entries;
   private int projectionsSize;
   private int untrackedEntries;

   protected IterationNextOperation(InternalRemoteCache<?, ?> cache, byte[] iterationId,
                                    KeyTracker segmentKeyTracker) {
      super(cache);
      this.iterationId = iterationId;
      this.segmentKeyTracker = segmentKeyTracker;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, iterationId);
   }

   @Override
   public IterationNextResponse<K, E> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (entriesSize < 0) {
         finishedSegments = ByteBufUtil.readArray(buf);
         entriesSize = ByteBufUtil.readVInt(buf);
         if (entriesSize == 0) {
            IntSet finishedSegmentSet = IntSets.from(finishedSegments);
            segmentKeyTracker.segmentsFinished(finishedSegmentSet);
            return new IterationNextResponse<>(status, Collections.emptyList(), finishedSegmentSet, false);
         }
         entries = new ArrayList<>(entriesSize);
         projectionsSize = -1;
         decoder.checkpoint();
      }

      if (projectionsSize < 0) {
         projectionsSize = ByteBufUtil.readVInt(buf);
         decoder.checkpoint();
      }

      ClassAllowList classAllowList = internalRemoteCache.getRemoteCacheContainer().getConfiguration().getClassAllowList();

      while (entries.size() + untrackedEntries < entriesSize) {
         short meta = buf.readUnsignedByte();
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         long version = 0;
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
            version = buf.readLong();
         }
         byte[] key = ByteBufUtil.readArray(buf);
         E value;
         if (projectionsSize > 1) {
            Object[] projections = new Object[projectionsSize];
            for (int j = 0; j < projectionsSize; j++) {
               projections[j] = unmarshaller.readValue(buf);
            }
            value = (E) projections;
         } else {
            value = unmarshaller.readValue(buf);
         }
         if (meta == 1) {
            value = (E) new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
         }

         if (segmentKeyTracker.track(key, status, classAllowList)) {
            K unmarshallKey = internalRemoteCache.getDataFormat().keyToObj(key, classAllowList);
            entries.add(new SimpleEntry<>(unmarshallKey, value));
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
      return new IterationNextResponse<>(status, entries, finishedSegmentSet, true);
   }

   @Override
   public short requestOpCode() {
      return ITERATION_NEXT_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ITERATION_NEXT_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }
}
