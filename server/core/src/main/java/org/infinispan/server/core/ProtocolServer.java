package org.infinispan.server.core;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

/**
 * Represents a protocol compliant server.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 9.0
 */
public interface ProtocolServer<C extends ProtocolServerConfiguration> extends CacheIgnoreAware {
   /**
    * Starts the server backed by the given cache manager and with the corresponding configuration.
    */
   void start(C configuration, EmbeddedCacheManager cacheManager);

   /**
    *  Stops the server
    */
   void stop();

   /**
    * Gets the encoder for this protocol server. The encoder is responsible for writing back common header responses
    * back to client. This method can return null if the server has no encoder. You can find an example of the server
    * that has no encoder in the Memcached server.
    */
   ChannelOutboundHandler getEncoder();

   /**
    * Gets the decoder for this protocol server. The decoder is responsible for reading client requests.
    * This method cannot return null.
    */
   ChannelInboundHandler getDecoder();

   /**
    * Returns the configuration used to start this server
    */
   C getConfiguration();

   /**
    * Returns a pipeline factory
    */
   ChannelInitializer<Channel> getInitializer();

   /**
    * Returns the name of this server
    */
   String getName();
}
