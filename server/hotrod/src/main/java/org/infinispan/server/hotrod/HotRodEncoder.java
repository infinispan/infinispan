package org.infinispan.server.hotrod;

import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.hotrod.counter.listener.ClientCounterEvent;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.internal.PlatformDependent;

/**
 * Hot Rod specific encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Sharable
class HotRodEncoder extends MessageToByteEncoder<Object> {
   private final static Log log = LogFactory.getLog(HotRodEncoder.class, Log.class);
   private final static boolean isTrace = log.isTraceEnabled();
   private final EmbeddedCacheManager cacheManager;
   private final HotRodServer server;
   private final boolean isClustered;
   private Cache<Address, ServerAddress> addressCache;

   HotRodEncoder(EmbeddedCacheManager cacheManager, HotRodServer server) {
      super(PlatformDependent.directBufferPreferred());
      this.cacheManager = cacheManager;
      this.server = server;
      isClustered = cacheManager.getCacheManagerConfiguration().transport().transport() != null;
   }

   private Cache<Address, ServerAddress> getAddressCache() {
      if (addressCache == null) {
         addressCache = isClustered ? cacheManager.getCache(server.getConfiguration().topologyCacheName()) : null;
      }
      return addressCache;
   }

   @Override
   protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf buf) throws Exception {
      try {
         if (isTrace) {
            log.tracef("Encode msg %s", msg);
         }

         if (msg instanceof Response) {
            Response r = (Response) msg;
            VersionedEncoder encoder = HotRodVersion.getEncoder(r.version);
            try {
               if (HotRodVersion.forVersion(r.version) != HotRodVersion.UNKNOWN) {
                  encoder.writeHeader(r, buf, getAddressCache(), server);
               } else {
                  // if error before reading version, don't send any topology changes
                  // cos the encoding might vary from one version to the other
                  encoder.writeHeader(r, buf, null, server);
               }

               encoder.writeResponse(r, buf, cacheManager, server);
            } catch (Throwable t) {
               log.errorWritingResponse(r.messageId, t);
               buf.clear(); // reset buffer
               ErrorResponse error = new ErrorResponse(r.version, r.messageId, r.cacheName, r.clientIntel,
                     OperationStatus.ServerError, r.topologyId, t.toString());
               encoder.writeHeader(error, buf, getAddressCache(), server);
               encoder.writeResponse(error, buf, cacheManager, server);
            }
         } else if (msg instanceof Events.Event) {
            Events.Event e = (Events.Event) msg;
            VersionedEncoder encoder = HotRodVersion.getEncoder(e.version);
            encoder.writeEvent(e, buf);
         } else if (msg instanceof ClientCounterEvent) {
            VersionedEncoder encoder = HotRodVersion.getEncoder(((ClientCounterEvent) msg).getVersion());
            encoder.writeCounterEvent((ClientCounterEvent) msg, buf);
         } else if (msg != null) {
            log.errorUnexpectedMessage(msg);
         }
      } catch (Throwable t) {
         log.errorEncodingMessage(msg, t);
         // When the exception is thrown when trying to encode an error response, all we can do
         // is to close the channel to signalize clients that the server has trouble responding.
         ctx.close();
         throw t;
      }
   }
}
