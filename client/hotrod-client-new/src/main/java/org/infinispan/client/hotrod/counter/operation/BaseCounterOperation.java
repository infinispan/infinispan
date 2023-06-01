package org.infinispan.client.hotrod.counter.operation;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.counter.impl.CounterOperationFactory;
import org.infinispan.client.hotrod.impl.operations.AbstractNoCacheHotRodOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.counter.exception.CounterException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * A base operation class for the counter's operation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
abstract class BaseCounterOperation<T> extends AbstractNoCacheHotRodOperation<T> {

   private static final Charset CHARSET = StandardCharsets.UTF_8;
   private final String counterName;
   private final boolean useConsistentHash;

   BaseCounterOperation(String counterName, boolean useConsistentHash) {
      this.counterName = counterName;
      this.useConsistentHash = useConsistentHash;
   }

   @Override
   public String getCacheName() {
      return CounterOperationFactory.COUNTER_CACHE_NAME;
   }

   @Override
   public byte[] getCacheNameBytes() {
      return RemoteCacheManager.cacheNameBytes(CounterOperationFactory.COUNTER_CACHE_NAME);
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeString(buf, counterName);
   }

   /**
    * If the status is {@link #KEY_DOES_NOT_EXIST_STATUS}, the counter is undefined and a {@link CounterException} is
    * thrown.
    */
   void checkStatus(short status) {
      if (status == KEY_DOES_NOT_EXIST_STATUS) {
         throw Log.HOTROD.undefinedCounter(counterName);
      }
   }

   @Override
   public Object getRoutingObject() {
      return useConsistentHash ? new ByteString(counterName) : null;
   }

   @Override
   protected void addParams(StringBuilder sb) {
      sb.append("counter=").append(counterName);
   }

   private static class ByteString {

      private final int hash;
      private final byte[] b;

      private ByteString(String s) {
         //copied from ByteString in core
         this.b = s.getBytes(CHARSET);
         this.hash = Arrays.hashCode(b);
      }

      @Override
      public int hashCode() {
         return hash;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ByteString that = (ByteString) o;
         return Arrays.equals(b, that.b);
      }

      @Override
      public String toString() {
         return new String(b, CHARSET);
      }
   }
}
