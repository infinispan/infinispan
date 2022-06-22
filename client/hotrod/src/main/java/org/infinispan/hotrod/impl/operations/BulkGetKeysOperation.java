package org.infinispan.hotrod.impl.operations;

import static org.infinispan.hotrod.marshall.MarshallerUtil.bytes2obj;

import java.util.HashSet;
import java.util.Set;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.protocol.Codec;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Reads all keys. Similar to <a href="http://community.jboss.org/wiki/HotRodBulkGet-Design">BulkGet</a>, but without
 * the entry values.
 *
 * @since 14.0
 */
public class BulkGetKeysOperation<K> extends StatsAffectingRetryingOperation<Set<K>> {
   private final int scope;
   private final Set<K> result = new HashSet<>();

   public BulkGetKeysOperation(OperationContext operationContext, CacheOptions options, int scope, DataFormat dataFormat) {
      super(operationContext, BULK_GET_KEYS_REQUEST, BULK_GET_KEYS_RESPONSE, options, dataFormat);
      this.scope = scope;
   }

   @Override
   protected void executeOperation(Channel channel) {
      scheduleRead(channel);
      Codec codec = operationContext.getCodec();
      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateVIntSize(scope));
      codec.writeHeader(buf, header);
      ByteBufUtil.writeVInt(buf, scope);
      channel.writeAndFlush(buf);
   }

   @Override
   protected void reset() {
      super.reset();
      result.clear();
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      while (buf.readUnsignedByte() == 1) { //there's more!
         result.add(bytes2obj(operationContext.getChannelFactory().getMarshaller(), ByteBufUtil.readArray(buf), dataFormat().isObjectStorage(), operationContext.getConfiguration().getClassAllowList()));
         decoder.checkpoint();
      }
      complete(result);
   }
}
