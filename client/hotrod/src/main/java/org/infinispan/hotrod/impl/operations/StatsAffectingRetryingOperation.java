package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.DataFormat;

import io.netty.channel.Channel;

/**
 * @since 14.0
 */
public abstract class StatsAffectingRetryingOperation<T> extends RetryOnFailureOperation<T> {
   private long startTime;

   protected StatsAffectingRetryingOperation(OperationContext operationContext, short requestCode, short responseCode, CacheOptions options, DataFormat dataFormat) {
      super(operationContext, requestCode, responseCode, options, dataFormat);
   }

   @Override
   protected void scheduleRead(Channel channel) {
      if (operationContext.getClientStatistics().isEnabled()) {
         startTime = operationContext.getClientStatistics().time();
      }
      super.scheduleRead(channel);
   }

   public void statsDataRead(boolean success) {
      if (operationContext.getClientStatistics().isEnabled()) {
         operationContext.getClientStatistics().dataRead(success, startTime, 1);
      }
   }

   protected void statsDataRead(boolean success, int count) {
      if (operationContext.getClientStatistics().isEnabled() && count > 0) {
         operationContext.getClientStatistics().dataRead(success, startTime, count);
      }
   }

   public void statsDataStore() {
      if (operationContext.getClientStatistics().isEnabled()) {
         operationContext.getClientStatistics().dataStore(startTime, 1);
      }
   }

   protected void statsDataStore(int count) {
      if (operationContext.getClientStatistics().isEnabled() && count > 0) {
         operationContext.getClientStatistics().dataStore(startTime, count);
      }
   }

   protected void statsDataRemove() {
      if (operationContext.getClientStatistics().isEnabled()) {
         operationContext.getClientStatistics().dataRemove(startTime, 1);
      }
   }
}
