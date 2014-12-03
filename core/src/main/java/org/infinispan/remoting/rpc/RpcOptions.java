package org.infinispan.remoting.rpc;

import org.infinispan.remoting.inboundhandler.DeliverOrder;

import java.util.concurrent.TimeUnit;

/**
 * Classes that wraps all the configuration parameters to configure a remote invocation.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class RpcOptions {

   private final long timeout;
   private final TimeUnit unit;
   private final ResponseFilter responseFilter;
   private final ResponseMode responseMode;
   private final DeliverOrder deliverOrder;
   private final boolean skipReplicationQueue;

   /**
    * @deprecated use instead {@link #RpcOptions(long, java.util.concurrent.TimeUnit, ResponseFilter, ResponseMode,
    * boolean, org.infinispan.remoting.inboundhandler.DeliverOrder)}
    */
   @Deprecated
   public RpcOptions(long timeout, TimeUnit unit, ResponseFilter responseFilter,
                     ResponseMode responseMode, boolean skipReplicationQueue, boolean fifoOrder, boolean totalOrder) {
      this(timeout, unit, responseFilter, responseMode, skipReplicationQueue,
           totalOrder ? DeliverOrder.TOTAL : (fifoOrder ? DeliverOrder.PER_SENDER : DeliverOrder.NONE));
   }

   public RpcOptions(long timeout, TimeUnit unit, ResponseFilter responseFilter, ResponseMode responseMode,
                     boolean skipReplicationQueue, DeliverOrder deliverOrder) {
      if (unit == null) {
         throw new IllegalArgumentException("TimeUnit cannot be null");
      } else if (responseMode == null) {
         throw new IllegalArgumentException("ResponseMode cannot be null");
      } else if (deliverOrder == null) {
         throw new IllegalArgumentException("DeliverMode cannot be null");
      }
      this.timeout = timeout;
      this.unit = unit;
      this.responseFilter = responseFilter;
      this.responseMode = responseMode;
      this.skipReplicationQueue = skipReplicationQueue;
      this.deliverOrder = deliverOrder;
   }

   /**
    * @return the timeout value to give up.
    */
   public long timeout() {
      return timeout;
   }

   /**
    * @return the {@link TimeUnit} in which the timeout value is.
    */
   public TimeUnit timeUnit() {
      return unit;
   }

   /**
    * @return {@code true} if the message is to be delivered in FIFO order.
    * @deprecated use instead {@link #deliverOrder()}.
    */
   @Deprecated
   public boolean fifoOrder() {
      return deliverOrder == DeliverOrder.PER_SENDER;
   }

   /**
    * @return {@code true} if the message is to be delivered in total order.
    * @deprecated use instead {@link #deliverOrder()}.
    */
   @Deprecated
   public boolean totalOrder() {
      return deliverOrder == DeliverOrder.TOTAL;
   }

   /**
    * @return the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    */
   public DeliverOrder deliverOrder() {
      return deliverOrder;
   }

   /**
    * @return the {@link ResponseFilter} to be used. Default is {@code null} meaning waiting for all or none responses
    * depending if the remote invocation is synchronous or asynchronous respectively.
    */
   public ResponseFilter responseFilter() {
      return responseFilter;
   }

   /**
    * @return the {@link ResponseMode} to handle with the responses.
    */
   public ResponseMode responseMode() {
      return responseMode;
   }

   /**
    * @return if {@code true}, the remote invocation will never be dispatch to the {@link
    * org.infinispan.remoting.ReplicationQueue}. However, only asynchronous remote invocation may be dispatched to the
    * {@link org.infinispan.remoting.ReplicationQueue}.
    */
   public boolean skipReplicationQueue() {
      return skipReplicationQueue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptions options = (RpcOptions) o;

      if (skipReplicationQueue != options.skipReplicationQueue) return false;
      if (timeout != options.timeout) return false;
      if (deliverOrder != options.deliverOrder) return false;
      if (responseFilter != null ? !responseFilter.equals(options.responseFilter) : options.responseFilter != null)
         return false;
      if (responseMode != options.responseMode) return false;
      if (unit != options.unit) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + unit.hashCode();
      result = 31 * result + deliverOrder.hashCode();
      result = 31 * result + (responseFilter != null ? responseFilter.hashCode() : 0);
      result = 31 * result + responseMode.hashCode();
      result = 31 * result + (skipReplicationQueue ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "RpcOptions{" +
            "timeout=" + timeout +
            ", unit=" + unit +
            ", deliverOrder=" + deliverOrder +
            ", responseFilter=" + responseFilter +
            ", responseMode=" + responseMode +
            ", skipReplicationQueue=" + skipReplicationQueue +
            '}';
   }
}
