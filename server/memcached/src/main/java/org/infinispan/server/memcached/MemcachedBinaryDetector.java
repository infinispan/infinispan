package org.infinispan.server.memcached;

import org.infinispan.server.core.MagicByteDetector;
import org.infinispan.server.memcached.binary.BinaryConstants;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;

public class MemcachedBinaryDetector extends MagicByteDetector {
   public static final String NAME = "memcached-binary-detector";

   public MemcachedBinaryDetector(MemcachedServer server) {
      super(server, BinaryConstants.MAGIC_REQ);
   }

   @Override
   public String getName() {
      return NAME;
   }

   @Override
   protected ChannelInitializer<Channel> getInitializer() {
      return ((MemcachedServer) server).getInitializer(MemcachedProtocol.BINARY);
   }
}
