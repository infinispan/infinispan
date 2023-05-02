package org.infinispan.client.hotrod.impl.operations;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextOperation<K, E> extends HotRodOperation<IterationNextResponse<K, E>> {

   private final byte[] iterationId;
   private final Channel channel;
   private final KeyTracker segmentKeyTracker;

   private byte[] finishedSegments;
   private int entriesSize = -1;
   private List<Entry<K, E>> entries;
   private int projectionsSize;
   private int untrackedEntries;

   protected IterationNextOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName,
                                    AtomicReference<ClientTopology> clientTopology, byte[] iterationId, Channel channel,
                                    ChannelFactory channelFactory, KeyTracker segmentKeyTracker,
                                    DataFormat dataFormat) {
      super(ITERATION_NEXT_REQUEST, ITERATION_NEXT_RESPONSE, codec, flags, cfg, cacheName, clientTopology, channelFactory, dataFormat);
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
            complete(new IterationNextResponse<>(status, Collections.emptyList(), finishedSegmentSet, false));
            return;
         }
         entries = new ArrayList<>(entriesSize);
         projectionsSize = -1;
         decoder.checkpoint();
      }

      if (projectionsSize < 0) {
         projectionsSize = codec.readProjectionSize(buf);
         decoder.checkpoint();
      }

      while (entries.size() + untrackedEntries < entriesSize) {
         short meta = codec.readMeta(buf);
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
               projections[j] = unmarshallValue(ByteBufUtil.readArray(buf));
            }
            value = (E) projections;
         } else {
            value = unmarshallValue(ByteBufUtil.readArray(buf));
         }
         if (meta == 1) {
            value = (E) new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
         }

         if (segmentKeyTracker.track(key, status, cfg.getClassAllowList())) {
            K unmarshallKey = dataFormat().keyToObj(key, cfg.getClassAllowList());
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
      complete(new IterationNextResponse<>(status, entries, finishedSegmentSet, entriesSize > 0));
   }

   private <M> M unmarshallValue(byte[] bytes) {
      return dataFormat().valueToObj(bytes, cfg.getClassAllowList());
   }

   @Override
   public void writeBytes(Channel channel, ByteBuf buf) {
      writeArrayOperation(buf, iterationId);
   }
}
