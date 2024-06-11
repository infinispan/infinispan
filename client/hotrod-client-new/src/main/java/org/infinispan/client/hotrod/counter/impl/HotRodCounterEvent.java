package org.infinispan.client.hotrod.counter.impl;

import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.api.CounterState;

/**
 * A {@link CounterEvent} implementation for the Hot Rod client.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class HotRodCounterEvent implements CounterEvent {

   private final byte[] listenerId;
   private final String counterName;
   private final long oldValue;
   private final CounterState oldState;
   private final long newValue;
   private final CounterState newState;

   public HotRodCounterEvent(byte[] listenerId, String counterName, long oldValue, CounterState oldState, long newValue,
                             CounterState newState) {
      this.listenerId = listenerId;
      this.counterName = counterName;
      this.oldValue = oldValue;
      this.oldState = oldState;
      this.newValue = newValue;
      this.newState = newState;
   }

   public byte[] getListenerId() {
      return listenerId;
   }

   public String getCounterName() {
      return counterName;
   }

   @Override
   public long getOldValue() {
      return oldValue;
   }

   @Override
   public CounterState getOldState() {
      return oldState;
   }

   @Override
   public long getNewValue() {
      return newValue;
   }

   @Override
   public CounterState getNewState() {
      return newState;
   }

   @Override
   public String toString() {
      return "HotRodCounterEvent{" +
             "counterName='" + counterName + '\'' +
             ", oldValue=" + oldValue +
             ", oldState=" + oldState +
             ", newValue=" + newValue +
             ", newState=" + newState +
             '}';
   }
}
