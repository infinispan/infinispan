package org.infinispan.hotrod.impl.counter.operation;

import java.util.ArrayList;
import java.util.Collection;

import org.infinispan.counter.api.CounterManager;
import org.infinispan.hotrod.impl.operations.OperationContext;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A counter operation for {@link CounterManager#getCounterNames()}.
 *
 * @since 14.0
 */
public class GetCounterNamesOperation extends BaseCounterOperation<Collection<String>> {
   private int size;
   private Collection<String> names;

   public GetCounterNamesOperation(OperationContext operationContext) {
      super(operationContext, COUNTER_GET_NAMES_REQUEST, COUNTER_GET_NAMES_RESPONSE, "", false);
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      sendHeader(channel);
      setCacheName();
   }

   @Override
   protected void reset() {
      super.reset();
      names = null;
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      assert status == NO_ERROR_STATUS;
      if (names == null) {
         size = ByteBufUtil.readVInt(buf);
         names = new ArrayList<>(size);
      }
      while (names.size() < size) {
         names.add(ByteBufUtil.readString(buf));
         decoder.checkpoint();
      }
      complete(names);
   }
}
