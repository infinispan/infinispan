package org.infinispan.hotrod.impl.multimap.operations;

import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_REQUEST;
import static org.infinispan.hotrod.impl.multimap.protocol.MultimapHotRodConstants.GET_MULTIMAP_RESPONSE;
import static org.infinispan.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import net.jcip.annotations.Immutable;

/**
 * Implements "get" for multimap as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @since 14.0
 */
@Immutable
public class GetKeyMultimapOperation<K, V> extends AbstractMultimapKeyOperation<K, Collection<V>> {
   private int size;
   private Collection<V> result;

   public GetKeyMultimapOperation(OperationContext operationContext,
                                  K key, byte[] keyBytes, CacheOptions options,
                                  DataFormat dataFormat, boolean supportsDuplicates) {
      super(operationContext, GET_MULTIMAP_REQUEST, GET_MULTIMAP_RESPONSE, key, keyBytes, options, dataFormat, supportsDuplicates);
   }

   @Override
   protected void reset() {
      super.reset();
      result = null;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status)) {
         complete(Collections.emptySet());
         return;
      } else if (result == null) {
         size = ByteBufUtil.readVInt(buf);
         result = supportsDuplicates ? new ArrayList<>(size) : new HashSet<>(size);
      }
      while (result.size() < size) {
         V value = bytes2obj(operationContext.getChannelFactory().getMarshaller(), ByteBufUtil.readArray(buf), dataFormat().isObjectStorage(), operationContext.getConfiguration().getClassAllowList());
         result.add(value);
         decoder.checkpoint();
      }
      complete(result);
   }
}
