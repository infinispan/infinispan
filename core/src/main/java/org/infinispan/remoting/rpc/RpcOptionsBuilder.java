package org.infinispan.remoting.rpc;

import org.infinispan.remoting.inboundhandler.DeliverOrder;

import java.util.concurrent.TimeUnit;

/**
 * It builds {@link RpcOptions} instances with the options to be used in remote invocations.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RpcOptionsBuilder {

   private long timeout;
   private TimeUnit unit;
   private DeliverOrder deliverOrder;
   private ResponseFilter responseFilter;
   private ResponseMode responseMode;
   private boolean skipReplicationQueue;

   /**
    * @deprecated use instead {@link #RpcOptionsBuilder(long, java.util.concurrent.TimeUnit, ResponseMode,
    * org.infinispan.remoting.inboundhandler.DeliverOrder)}
    */
   @Deprecated
   public RpcOptionsBuilder(long timeout, TimeUnit unit, ResponseMode responseMode, boolean fifoOrder) {
      this(timeout, unit, responseMode, fifoOrder ? DeliverOrder.PER_SENDER : DeliverOrder.NONE);
   }

   public RpcOptionsBuilder(long timeout, TimeUnit unit, ResponseMode responseMode, DeliverOrder deliverOrder) {
      this.timeout = timeout;
      this.unit = unit;
      this.deliverOrder = deliverOrder;
      this.responseFilter = null;
      this.responseMode = responseMode;
      this.skipReplicationQueue = false;
   }

   /**
    * Constructor based on existing {@link RpcOptions}
    *
    * @param template an existing {@link RpcOptions}
    */
   public RpcOptionsBuilder(RpcOptions template) {
      this.timeout = template.timeout();
      this.unit = template.timeUnit();
      this.deliverOrder = template.deliverOrder();
      this.responseFilter = template.responseFilter();
      this.responseMode = template.responseMode();
      this.skipReplicationQueue = template.skipReplicationQueue();
   }

   /**
    * Sets the timeout value and its {@link TimeUnit}.
    *
    * @param timeout timeout value
    * @param unit    the {@link TimeUnit} of timeout value
    * @return this instance
    */
   public RpcOptionsBuilder timeout(long timeout, TimeUnit unit) {
      this.timeout = timeout;
      this.unit = unit;
      return this;
   }

   /**
    * See {@link #timeout(long, java.util.concurrent.TimeUnit)}
    *
    * @return the timeout in {@link TimeUnit}.
    */
   public long timeout(TimeUnit outputTimeUnit) {
      return outputTimeUnit.convert(timeout, unit);
   }

   /**
    * Note: this option may be set to {@code false} if by the current {@link org.infinispan.remoting.transport.Transport}
    * if the {@link org.infinispan.remoting.rpc.ResponseMode#isSynchronous()} returns {@code true}.
    *
    * @param fifoOrder if {@code true}, it the message will be deliver in First In, First Out (FIFO) order with other
    *                  sent message with FIFO. Otherwise, the message is delivered as soon as it is received.
    * @return this instance
    * @deprecated use instead {@link #deliverMode(org.infinispan.remoting.inboundhandler.DeliverOrder)}.
    */
   @Deprecated
   public RpcOptionsBuilder fifoOrder(boolean fifoOrder) {
      if (deliverOrder != DeliverOrder.TOTAL) {
         this.deliverOrder = fifoOrder ? DeliverOrder.PER_SENDER : DeliverOrder.NONE;
      }
      return this;
   }

   /**
    * @param totalOrder if {@code true}, the message will be delivered by a global order, i.e., the same order in all
    *                   the nodes
    * @return this instance
    * @deprecated use instead {@link #deliverMode(org.infinispan.remoting.inboundhandler.DeliverOrder)}.
    */
   @Deprecated
   public RpcOptionsBuilder totalOrder(boolean totalOrder) {
      if (totalOrder) {
         this.deliverOrder = DeliverOrder.TOTAL;
      }
      return this;
   }

   /**
    * @param deliverOrder the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    * @return this instance.
    */
   public RpcOptionsBuilder deliverMode(DeliverOrder deliverOrder) {
      this.deliverOrder = deliverOrder;
      return this;
   }

   /**
    * Sets the {@link ResponseFilter}. {@code null} by default, meaning it will wait for all or none response depending
    * if the remote invocation is synchronous or asynchronous respectively.
    *
    * @param responseFilter
    * @return this instance
    */
   public RpcOptionsBuilder responseFilter(ResponseFilter responseFilter) {
      this.responseFilter = responseFilter;
      return this;
   }

   /**
    * Sets the {@link ResponseMode} for the remote invocation. See {@link ResponseMode} documentation to see the
    * available values.
    *
    * @param responseMode
    * @return this instance
    */
   public RpcOptionsBuilder responseMode(ResponseMode responseMode) {
      this.responseMode = responseMode;
      return this;
   }

   /**
    * Sets if the remote invocation must skip the {@link org.infinispan.remoting.ReplicationQueue}.
    * <p/>
    * Note1: only asynchronous remote invocation may be dispatched to the {@link org.infinispan.remoting.ReplicationQueue}.
    * <p/>
    * Node2: {@code false} by default
    *
    * @param skipReplicationQueue {@code true} to force skip the {@link org.infinispan.remoting.ReplicationQueue}
    * @return this instance
    */
   public RpcOptionsBuilder skipReplicationQueue(boolean skipReplicationQueue) {
      this.skipReplicationQueue = skipReplicationQueue;
      return this;
   }

   /**
    * @return an instance of {@link RpcOptions} with the current builder options
    */
   public final RpcOptions build() {
      return new RpcOptions(timeout, unit, responseFilter, responseMode, skipReplicationQueue, deliverOrder);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptionsBuilder that = (RpcOptionsBuilder) o;

      if (skipReplicationQueue != that.skipReplicationQueue) return false;
      if (timeout != that.timeout) return false;
      if (deliverOrder != that.deliverOrder) return false;
      if (responseFilter != null ? !responseFilter.equals(that.responseFilter) : that.responseFilter != null)
         return false;
      if (responseMode != that.responseMode) return false;
      if (unit != that.unit) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + unit.hashCode();
      result = 31 * result + deliverOrder.hashCode();
      result = 31 * result + (responseFilter != null ? responseFilter.hashCode() : 0);
      result = 31 * result + (responseMode != null ? responseMode.hashCode() : 0);
      result = 31 * result + (skipReplicationQueue ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "RpcOptionsBuilder{" +
            "timeout=" + timeout +
            ", unit=" + unit +
            ", deliverOrder=" + deliverOrder +
            ", responseFilter=" + responseFilter +
            ", responseMode=" + responseMode +
            ", skipReplicationQueue=" + skipReplicationQueue +
            '}';
   }
}
