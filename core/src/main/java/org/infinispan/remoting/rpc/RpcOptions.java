package org.infinispan.remoting.rpc;

import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.inboundhandler.DeliverOrder;

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

   /**
    * @since 9.2
    */
   @SuppressWarnings("deprecation")
   public RpcOptions(DeliverOrder deliverOrder, long timeout, TimeUnit unit) {
      this(timeout, unit, null, ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, deliverOrder);
   }

   /**
    * @deprecated Since 9.0, use {@link #RpcOptions(long, TimeUnit, ResponseFilter, ResponseMode, DeliverOrder)} instead.
    */
   @Deprecated
   public RpcOptions(long timeout, TimeUnit unit, ResponseFilter responseFilter, ResponseMode responseMode,
         boolean skipReplicationQueue, DeliverOrder deliverOrder) {
      this(timeout, unit, responseFilter, responseMode, deliverOrder);
   }

   /**
    * @deprecated Since 9.2, use {@link #RpcOptions(DeliverOrder, long, TimeUnit)} instead.
    */
   @Deprecated
   public RpcOptions(long timeout, TimeUnit unit, ResponseFilter responseFilter, ResponseMode responseMode,
         DeliverOrder deliverOrder) {
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
    * @return the {@link org.infinispan.remoting.inboundhandler.DeliverOrder}.
    */
   public DeliverOrder deliverOrder() {
      return deliverOrder;
   }

   /**
    * @return the {@link ResponseFilter} to be used. Default is {@code null} meaning waiting for all or none responses
    * depending if the remote invocation is synchronous or asynchronous respectively.
    * @deprecated Since 9.2, ignored by {@code RpcManager.invokeCommand*()}.
    */
   public ResponseFilter responseFilter() {
      return responseFilter;
   }

   /**
    * @return the {@link ResponseMode} to handle with the responses.
    * @deprecated Since 9.2, ignored by {@code RpcManager.invokeCommand*()}.
    */
   @Deprecated
   public ResponseMode responseMode() {
      return responseMode;
   }

   /**
    * @deprecated Since 9.0, always returns {@code false}.
    */
   @Deprecated
   public boolean skipReplicationQueue() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptions options = (RpcOptions) o;

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
            '}';
   }
}
