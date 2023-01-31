package org.infinispan.server.core;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.transport.Transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;

/**
 * Represents a protocol compliant server.
 *
 * @author Galder Zamarreño
 * @author wburns
 * @since 9.0
 */
public interface ProtocolServer<C extends ProtocolServerConfiguration> {

   /**
    * Starts the server backed by the given cache manager, with the corresponding configuration. The cache manager is
    * expected to be completely initialized and started prior to this call.
    */
   void start(C configuration, EmbeddedCacheManager cacheManager);

   /**
    * Stops the server.
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

   /**
    * Returns the transport for this server
    */
   Transport getTransport();

   /**
    * Sets the {@link ServerManagement} instance for this protocol server
    */
   void setServerManagement(ServerManagement server, boolean adminEndpoint);

   /**
    * Sets the enclosing {@link ProtocolServer}. Used by the single port server
    */
   void setEnclosingProtocolServer(ProtocolServer<?> enclosingProtocolServer);

   /**
    * Returns the enclosing {@link ProtocolServer}. May be null if this server has none.
    */
   ProtocolServer<?> getEnclosingProtocolServer();

   /**
    * Returns a {@link ChannelMatcher} which matches channels which belong to this protocol server
    */
   ChannelMatcher getChannelMatcher();

   /**
    * Installs a protocol detector on the channel
    * @param ch
    */
   void installDetector(Channel ch);
}
