package org.infinispan.client.hotrod.impl.multimap.operations;

import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_REQUEST;
import static org.infinispan.client.hotrod.impl.multimap.protocol.MultimapHotRodConstants.PUT_MULTIMAP_RESPONSE;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.exceptions.InvalidResponseException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import com.google.errorprone.annotations.Immutable;

import io.netty.buffer.ByteBuf;

/**
 * Implements "put" for multimap cache as defined by  <a href="http://community.jboss.org/wiki/HotRodProtocol">Hot Rod
 * protocol specification</a>.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@Immutable
public class PutKeyValueMultimapOperation extends AbstractMultimapKeyValueOperation<Void> {

   public PutKeyValueMultimapOperation(InternalRemoteCache<?, ?> remoteCache, byte[] keyBytes, byte[] value, long lifespan,
                                       TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      super(remoteCache, keyBytes, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, supportsDuplicates);
   }

   @Override
   public Void createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.isSuccess(status)) {
         throw new InvalidResponseException("Unexpected response status: " + Integer.toHexString(status));
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return PUT_MULTIMAP_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return PUT_MULTIMAP_RESPONSE;
   }
}
