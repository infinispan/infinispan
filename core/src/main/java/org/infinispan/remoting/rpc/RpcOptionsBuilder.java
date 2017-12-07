package org.infinispan.remoting.rpc;

import java.util.concurrent.TimeUnit;

import org.infinispan.remoting.inboundhandler.DeliverOrder;

/**
 * It builds {@link RpcOptions} instances with the options to be used in remote invocations.
 *
 * @author Pedro Ruivo
 * @since 5.3
 * @deprecated Since 9.2, please use {@link RpcOptions} directly.
 */
@Deprecated
public class RpcOptionsBuilder {

   private long timeout;
   private TimeUnit unit;
   private DeliverOrder deliverOrder;
   private ResponseFilter responseFilter;
   private ResponseMode responseMode;

   public RpcOptionsBuilder(long timeout, TimeUnit unit, ResponseMode responseMode, DeliverOrder deliverOrder) {
      this.timeout = timeout;
      this.unit = unit;
      this.deliverOrder = deliverOrder;
      this.responseFilter = null;
      this.responseMode = responseMode;
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
    * @deprecated Since 9.2, ignored by {@code RpcManager.invokeCommand*()}.
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
    * @deprecated Since 9.2, ignored by {@code RpcManager.invokeCommand*()}.
    */
   public RpcOptionsBuilder responseMode(ResponseMode responseMode) {
      this.responseMode = responseMode;
      return this;
   }

   /**
    * @deprecated Since 9.0, it no longer does anything.
    */
   @Deprecated
   public RpcOptionsBuilder skipReplicationQueue(boolean skipReplicationQueue) {
      return this;
   }

   /**
    * @return an instance of {@link RpcOptions} with the current builder options
    */
   public final RpcOptions build() {
      return new RpcOptions(timeout, unit, responseFilter, responseMode, deliverOrder);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RpcOptionsBuilder that = (RpcOptionsBuilder) o;

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
            '}';
   }
}
