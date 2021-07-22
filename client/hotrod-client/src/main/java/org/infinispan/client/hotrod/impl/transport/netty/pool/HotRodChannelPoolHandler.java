package org.infinispan.client.hotrod.impl.transport.netty.pool;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.impl.transport.netty.ChannelInitializer;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelKeys;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelRecord;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelPoolHandler;

public class HotRodChannelPoolHandler implements ChannelPoolHandler {

   private static final Log log = LogFactory.getLog(HotRodChannelPoolHandler.class);

   private final AtomicInteger channelCount = new AtomicInteger(0);
   private final AtomicInteger acquiredCount = new AtomicInteger(0);

   private final ChannelInitializer channelInitializer;

   public HotRodChannelPoolHandler(ChannelInitializer channelInitializer) {
      this.channelInitializer = channelInitializer;
   }

   @Override
   public void channelCreated(Channel ch) throws Exception {
      this.channelInitializer.initChannel(ch);
      channelCount.incrementAndGet();
   }

   @Override
   public void channelReleased(Channel channel) {
      int currentActive = acquiredCount.decrementAndGet();
      if (log.isTraceEnabled()) log.tracef("[%s] Released channel %s, active = %d", ChannelKeys.getUnresolvedAddress(channel), channel, currentActive);
      if (currentActive < 0) {
         HOTROD.warnf("[%s] Invalid active count after releasing channel %s", ChannelKeys.getUnresolvedAddress(channel), channel);
      }
   }

   @Override
   public void channelAcquired(Channel channel) {
      acquiredCount.incrementAndGet();
      activateChannel(channel);
      ChannelRecord record = ChannelKeys.getChannelRecord(channel);
      record.setAcquired();
   }

   public int getChannelCreated() {
      return channelCount.get();
   }

   public int getChannelAcquired() {
      return acquiredCount.get();
   }

   public void decreasePoolCounters() {
      acquiredCount.decrementAndGet();
      channelCount.decrementAndGet();
   }

   public void decreaseChannelCreated() {
      channelCount.decrementAndGet();
   }

   private void activateChannel(Channel channel) {
      assert channel.isActive() : "Channel " + channel + " is not active";
      int currentActive = getChannelAcquired();
      if (log.isTraceEnabled()) log.tracef("[%s] Activated record %s, created = %d, active = %d", ChannelKeys.getUnresolvedAddress(channel), channel, getChannelCreated(), currentActive);
   }
}
