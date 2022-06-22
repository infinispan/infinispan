package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.Util;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * ExecuteOperation.
 *
 * @since 14.0
 */
public class ExecuteOperation<T> extends RetryOnFailureOperation<T> {

   private final String taskName;
   private final Map<String, byte[]> marshalledParams;
   private final Object key;

   protected ExecuteOperation(OperationContext operationContext, CacheOptions options, String taskName, Map<String, byte[]> marshalledParams, Object key, DataFormat dataFormat) {
      super(operationContext, EXEC_REQUEST, EXEC_RESPONSE, options, dataFormat);
      this.taskName = taskName;
      this.marshalledParams = marshalledParams;
      this.key = key;
   }

   @Override
   protected void fetchChannelAndInvoke(int retryCount, Set<SocketAddress> failedServers) {
      if (key != null) {
         operationContext.getChannelFactory().fetchChannelAndInvoke(key, failedServers, operationContext.getCacheNameBytes(), this);
      } else {
         operationContext.getChannelFactory().fetchChannelAndInvoke(failedServers, operationContext.getCacheNameBytes(), this);
      }
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(); // estimation too complex

      operationContext.getCodec().writeHeader(buf, header);
      ByteBufUtil.writeString(buf, taskName);
      ByteBufUtil.writeVInt(buf, marshalledParams.size());
      for (Entry<String, byte[]> entry : marshalledParams.entrySet()) {
         ByteBufUtil.writeString(buf, entry.getKey());
         ByteBufUtil.writeArray(buf, entry.getValue());
      }
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      complete(bytes2obj(operationContext.getChannelFactory().getMarshaller(), ByteBufUtil.readArray(buf), dataFormat().isObjectStorage(), operationContext.getConfiguration().getClassAllowList()));
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append(", taskName=").append(taskName);
      sb.append(", params=[");
      for (Iterator<Entry<String, byte[]>> iterator = marshalledParams.entrySet().iterator(); iterator.hasNext(); ) {
         Entry<String, byte[]> entry = iterator.next();
         String name = entry.getKey();
         byte[] value = entry.getValue();
         sb.append(name).append("=").append(Util.toStr(value));
         if (iterator.hasNext()) {
            sb.append(", ");
         }
      }
      sb.append("]");
   }
}
