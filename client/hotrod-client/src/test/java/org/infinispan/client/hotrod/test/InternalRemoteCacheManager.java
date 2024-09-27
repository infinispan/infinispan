package org.infinispan.client.hotrod.test;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.protocol.CodecHolder;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.client.hotrod.impl.transport.netty.TestChannelFactory;

/**
 * RemoteCacheManager that exposes internal components such as transportFactory.
 *
 * This class serves testing purposes and is NOT part of public API.
 *
 * @author Martin Gencur
 */
public class InternalRemoteCacheManager extends RemoteCacheManager {

   private final boolean testReplay;
   private ChannelFactory customChannelFactory;

   public InternalRemoteCacheManager(boolean testReplay, Configuration configuration) {
      super(configuration, true);
      this.testReplay = testReplay;
   }

   public InternalRemoteCacheManager(Configuration configuration) {
      super(configuration, true);
      this.testReplay = true;
   }

   public InternalRemoteCacheManager(Configuration configuration, ChannelFactory customChannelFactory) {
      this(configuration, false);
      this.customChannelFactory = customChannelFactory;
   }

   public InternalRemoteCacheManager(Configuration configuration, boolean start) {
      super(configuration, start);
      this.testReplay = true;
   }

   public InternalRemoteCacheManager(boolean start) {
      super(start);
      this.testReplay = true;
   }

   public InternalRemoteCacheManager() {
      this(true);
   }

   public ChannelFactory getChannelFactory() {
      return channelFactory;
   }

   @Override
   public ChannelFactory createChannelFactory() {
      if (customChannelFactory != null) return customChannelFactory;
      if (testReplay) return new TestChannelFactory(getConfiguration(), new CodecHolder(getConfiguration().version().getCodec()));
      return super.createChannelFactory();
   }
}
