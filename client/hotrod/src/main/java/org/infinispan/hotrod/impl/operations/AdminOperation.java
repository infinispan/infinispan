package org.infinispan.hotrod.impl.operations;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;

/**
 * AdminOperation. A special type of {@link ExecuteOperation} which returns the result of an admin operation which is
 * always represented as a JSON object. The actual parsing and interpretation of the result is up to the caller.
 *
 * @since 14.0
 */
public class AdminOperation extends ExecuteOperation<String> {
   AdminOperation(OperationContext operationContext, CacheOptions options, String taskName, Map<String, byte[]> marshalledParams) {
      super(operationContext, options, taskName, marshalledParams, null, null);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      byte[] bytes = ByteBufUtil.readArray(buf);
      complete(new String(bytes, StandardCharsets.UTF_8));
   }
}
