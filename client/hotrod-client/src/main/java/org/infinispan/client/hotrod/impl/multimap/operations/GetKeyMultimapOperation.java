package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "get" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class GetKeyMultimapOperation<V> extends AbstractMultimapKeyOperation<Collection<V>> {
   private int size;
   private Collection<V> result;

   public GetKeyMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, boolean supportsDuplicates) {
      super(remoteCache, keyBytes, supportsDuplicates);
   }

   @Override
   public Collection<V> createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isNotExist(status)) {
         return Collections.emptySet();
      } else if (result == null) {
         size = ByteBufUtil.readVInt(buf);
         result = supportsDuplicates ? new ArrayList<>(size) : new HashSet<>(size);
         decoder.checkpoint();
      }
      while (result.size() < size) {
         V value = unmarshaller.readValue(buf);
         result.add(value);
         decoder.checkpoint();
      }
      return result;
   }

   @Override
   public short requestOpCode() {
      return GET_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return GET_MULTIMAP_RESPONSE;
   }

   @Override
   public void reset() {
      result = null;
   }
}
