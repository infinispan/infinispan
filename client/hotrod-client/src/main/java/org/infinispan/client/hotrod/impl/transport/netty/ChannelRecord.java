package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

import io.netty.channel.Channel;

public class ChannelRecord {
   private static final Log log = LogFactory.getLog(ChannelRecord.class);

   private boolean closed = false;
   private boolean acquired = false;

   ChannelRecord() {
   }

   public synchronized void setAcquired() {
      assert !acquired;
      acquired = true;
   }

   public synchronized boolean isIdle() {
      return !acquired;
   }

   public synchronized boolean setIdleAndIsClosed() {
      assert acquired;
      acquired = false;

      return closed;
   }

   public synchronized boolean closeAndWasIdle() {
      assert !closed;
      closed = true;

      return !acquired;
   }

   public boolean release(Channel channel) {
      // The channel can be closed when it's idle (due to idle timeout or closed connection)
      if (this.isIdle()) {
         HOTROD.warnf("[%s] Cannot release channel %s because it is idle", ChannelKeys.getUnresolvedAddress(channel), channel);
         return false;
      }

      if (this.setIdleAndIsClosed()) {
         if (log.isTraceEnabled()) log.tracef("[%s] Attempt to release already closed channel %s", ChannelKeys.getUnresolvedAddress(channel), channel);
         return false;
      }
      return true;
   }
}
