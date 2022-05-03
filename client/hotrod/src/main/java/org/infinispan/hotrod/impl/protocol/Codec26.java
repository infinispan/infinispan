package org.infinispan.hotrod.impl.protocol;

import java.util.EnumSet;

import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec26 extends Codec25 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_26);
   }

   @Override
   public void writeClientListenerInterests(ByteBuf buf, EnumSet<CacheEntryEventType> types) {
      byte listenerInterests = 0;
      if (types.contains(CacheEntryEventType.CREATED))
         listenerInterests = (byte) (listenerInterests | 0x01);
      if (types.contains(CacheEntryEventType.UPDATED))
         listenerInterests = (byte) (listenerInterests | 0x02);
      if (types.contains(CacheEntryEventType.REMOVED))
         listenerInterests = (byte) (listenerInterests | 0x04);
      if (types.contains(CacheEntryEventType.EXPIRED))
         listenerInterests = (byte) (listenerInterests | 0x08);

      ByteBufUtil.writeVInt(buf, listenerInterests);
   }
}
