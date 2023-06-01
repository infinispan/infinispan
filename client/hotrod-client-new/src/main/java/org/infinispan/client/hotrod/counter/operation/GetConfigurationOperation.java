package org.infinispan.client.hotrod.counter.operation;

import static org.infinispan.counter.api._private.CounterEncodeUtil.decodeConfiguration;

import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;

import io.netty.buffer.ByteBuf;

/**
 * A counter configuration for {@link CounterManager#getConfiguration(String)}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class GetConfigurationOperation extends BaseCounterOperation<CounterConfiguration> {

   public GetConfigurationOperation(String counterName) {
      super(counterName, false);
   }

   @Override
   public CounterConfiguration createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (status != NO_ERROR_STATUS) {
         return null;
      }

      return decodeConfiguration(buf::readByte, buf::readLong, () -> ByteBufUtil.readVInt(buf));
   }

   @Override
   public short requestOpCode() {
      return COUNTER_GET_CONFIGURATION_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return COUNTER_GET_CONFIGURATION_RESPONSE;
   }
}
