package org.infinispan.server.hotrod;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.Events.Event;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;

import io.netty.buffer.ByteBuf;

/**
 * This class represents the work to be done by an encoder of a particular Hot Rod protocol version.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
interface VersionedEncoder {

   /**
    * Write the header to the given channel buffer
    */
   void writeHeader(Response r, ByteBuf buf, Cache<Address, ServerAddress> c, HotRodServer server);

   /**
    * Write operation response using the given channel buffer
    */
   void writeResponse(Response r, ByteBuf buf, EmbeddedCacheManager cacheManager, HotRodServer server);

   /**
    * Write an event, including its header, using the given channel buffer
    */
   void writeEvent(Event e, ByteBuf buf);

   /**
    * Writes a {@link ClientCounterEvent}, including its header, using a giver channel buffer.
    */
   void writeCounterEvent(ClientCounterEvent event, ByteBuf buffer);
}
