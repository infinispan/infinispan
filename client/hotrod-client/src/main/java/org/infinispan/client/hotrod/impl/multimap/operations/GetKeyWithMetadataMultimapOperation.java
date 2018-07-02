package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_WITH_METADATA_RESPONSE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.multimap.metadata.MetadataCollectionImpl;
import org.infinispan.client.hotrod.impl.operations.AbstractKeyOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Implements "getWithMetadata" as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod protocol
 * specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class GetKeyWithMetadataMultimapOperation<V> extends AbstractKeyOperation<MetadataCollection<V>> {
   private static final Log log = LogFactory.getLog(GetKeyWithMetadataMultimapOperation.class);
   private static final boolean trace = log.isTraceEnabled();

   public GetKeyWithMetadataMultimapOperation(Codec codec, ChannelFactory channelFactory,
                                              Object key, byte[] keyBytes, byte[] cacheName, AtomicInteger topologyId, int flags,
                                              Configuration cfg) {
      super(GET_MULTIMAP_WITH_METADATA_REQUEST, GET_MULTIMAP_WITH_METADATA_RESPONSE, codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg, null);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendArrayOperation(channel, keyBytes);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(new MetadataCollectionImpl<>(Collections.emptySet()));
         return;
      }
      if (!HotRodConstants.isSuccess(status)) {
         complete(null);
         return;
      }
      short flags = buf.readByte();
      long creation = -1;
      int lifespan = -1;
      long lastUsed = -1;
      int maxIdle = -1;
      if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
         creation = buf.readLong();
         lifespan = ByteBufUtil.readVInt(buf);
      }
      if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
         lastUsed = buf.readLong();
         maxIdle = ByteBufUtil.readVInt(buf);
      }
      long version = buf.readLong();
      if (trace) {
         log.tracef("Received version: %d", version);
      }
      int size = ByteBufUtil.readVInt(buf);
      Collection<V> values = new ArrayList<>(size);
      for (int i = 0; i < size; ++i) {
         V value = codec.readUnmarshallByteArray(buf, status, cfg.getClassWhiteList(), channelFactory.getMarshaller());
         values.add(value);
      }
      complete(new MetadataCollectionImpl<>(values, creation, lifespan, lastUsed, maxIdle, version));
   }
}
