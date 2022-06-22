package org.infinispan.client.hotrod.impl.operations;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * AdminOperation. A special type of {@link ExecuteOperation} which returns the result of an admin operation which is always
 * represented as a JSON object. The actual parsing and interpretation of the result is up to the caller.
 *
 * @author Tristan Tarrant
 * @since 9.3
 */
public class AdminOperation extends ExecuteOperation<String> {
   AdminOperation(Codec codec, ChannelFactory channelFactory, byte[] cacheName, AtomicReference<ClientTopology> clientTopology, int flags, Configuration cfg, String taskName, Map<String, byte[]> marshalledParams) {
      super(codec, channelFactory, cacheName, clientTopology, flags, cfg, taskName, marshalledParams, null, null);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      byte[] bytes = ByteBufUtil.readArray(buf);
      complete(new String(bytes, StandardCharsets.UTF_8));
   }
}
