package org.infinispan.client.hotrod.impl.protocol;

import java.lang.annotation.Annotation;
import java.util.Set;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 8.2
 */
public class Codec26 extends Codec25 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_26);
   }

   @Override
   public void writeClientListenerInterests(ByteBuf buf, Set<Class<? extends Annotation>> classes) {
      byte listenerInterests = 0;
      if (classes.contains(ClientCacheEntryCreated.class))
         listenerInterests = (byte) (listenerInterests | 0x01);
      if (classes.contains(ClientCacheEntryModified.class))
         listenerInterests = (byte) (listenerInterests | 0x02);
      if (classes.contains(ClientCacheEntryRemoved.class))
         listenerInterests = (byte) (listenerInterests | 0x04);
      if (classes.contains(ClientCacheEntryExpired.class))
         listenerInterests = (byte) (listenerInterests | 0x08);

      ByteBufUtil.writeVInt(buf, listenerInterests);
   }

}
