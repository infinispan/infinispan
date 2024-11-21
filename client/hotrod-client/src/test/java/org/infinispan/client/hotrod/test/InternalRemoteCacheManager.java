package org.infinispan.client.hotrod.test;

import java.util.function.Consumer;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;

import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.FixedLengthFrameDecoder;

/**
 * RemoteCacheManager that exposes internal components such as transportFactory.
 *
 * This class serves testing purposes and is NOT part of public API.
 *
 * @author Martin Gencur
 */
public class InternalRemoteCacheManager extends RemoteCacheManager {

   private final boolean testReplay;

   public InternalRemoteCacheManager(boolean testReplay, Configuration configuration) {
      super(configuration, true);
      this.testReplay = testReplay;
   }

   public InternalRemoteCacheManager(Configuration configuration) {
      super(configuration, true);
      this.testReplay = true;
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

   @Override
   protected Consumer<ChannelPipeline> pipelineWrapper() {
      return testReplay ? pipeline -> pipeline.addFirst("1frame", new FixedLengthFrameDecoder(1)) :
            super.pipelineWrapper();
   }
}
