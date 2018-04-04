package org.infinispan.client.hotrod.impl.operations;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextOperation<E> extends HotRodOperation<IterationNextResponse<E>> {

   private static final Log log = LogFactory.getLog(IterationNextOperation.class);

   private final byte[] iterationId;
   private final Channel channel;
   private final KeyTracker segmentKeyTracker;

   private byte[] finishedSegments;
   private int entriesSize = -1;
   private List<Entry<Object, E>> entries;
   private int projectionsSize;
   private int untrackedEntries;

   protected IterationNextOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName,
                                    AtomicInteger topologyId, byte[] iterationId, Channel channel,
                                    ChannelFactory channelFactory, KeyTracker segmentKeyTracker) {
      super(ITERATION_NEXT_REQUEST, ITERATION_NEXT_RESPONSE, codec, flags, cfg, cacheName, topologyId, channelFactory);
      this.iterationId = iterationId;
      this.channel = channel;
      this.segmentKeyTracker = segmentKeyTracker;
   }

   @Override
   public CompletableFuture<IterationNextResponse<E>> execute() {
      if (!channel.isActive()) {
         throw log.channelInactive(channel.remoteAddress(), channel.remoteAddress());
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
            segmentKeyTracker.segmentsFinished(finishedSegments);
            complete(new IterationNextResponse(status, Collections.emptyList(), false));
            return;
         }
         entries = new ArrayList<>(entriesSize);
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
         Object value;
         if (projectionsSize > 1) {
            Object[] projections = new Object[projectionsSize];
            for (int j = 0; j < projectionsSize; j++) {
               projections[j] = unmarshall(ByteBufUtil.readArray(buf), status);
            }
            value = projections;
         } else {
            value = unmarshall(ByteBufUtil.readArray(buf), status);
         }
         if (meta == 1) {
            value = new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
         }

         if (segmentKeyTracker.track(key, status, cfg.serialWhitelist())) {
            entries.add(new SimpleEntry<>(unmarshall(key, status), (E) value));
         } else {
            untrackedEntries++;
         }
         decoder.checkpoint();
      }
      segmentKeyTracker.segmentsFinished(finishedSegments);
      if (HotRodConstants.isInvalidIteration(status)) {
         throw log.errorRetrievingNext(new String(iterationId, HOTROD_STRING_CHARSET));
      }
      complete(new IterationNextResponse(status, entries, entriesSize > 0));
   }

   private Object unmarshall(byte[] bytes, short status) {
      Marshaller marshaller = channelFactory.getMarshaller();
      return MarshallerUtil.bytes2obj(marshaller, bytes, status, cfg.serialWhitelist());
   }
}
