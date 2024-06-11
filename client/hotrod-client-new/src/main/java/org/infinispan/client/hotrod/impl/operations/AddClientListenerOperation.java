package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.impl.ClientEventDispatcher;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author Galder Zamarreño
 */
public class AddClientListenerOperation extends ClientListenerOperation {

   // TODO: do we need to store it here?
   private final byte[][] filterFactoryParams;
   private final byte[][] converterFactoryParams;

   protected AddClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super(remoteCache, listener);
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
   }

   private AddClientListenerOperation(InternalRemoteCache<?, ?> remoteCache, Object listener, byte[] listenerId,
                                        byte[][] filterFactoryParams, byte[][] converterFactoryParams) {
      super(remoteCache, listener, listenerId);
      this.filterFactoryParams = filterFactoryParams;
      this.converterFactoryParams = converterFactoryParams;
   }


   @Override
   public ClientListenerOperation copy() {
      return new AddClientListenerOperation(internalRemoteCache, listener, listenerId, filterFactoryParams, converterFactoryParams);
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ClientListener clientListener = extractClientListener();
      ByteBufUtil.writeArray(buf, listenerId);
      codec.writeClientListenerParams(buf, clientListener, filterFactoryParams, converterFactoryParams);
      codec.writeClientListenerInterests(buf, ClientEventDispatcher.findMethods(listener).keySet());
   }

   @Override
   public Channel createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec,
                                       CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.isSuccess(status)) {
         return decoder.getChannel();
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return ADD_CLIENT_LISTENER_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return ADD_CLIENT_LISTENER_RESPONSE;
   }
}
