package org.infinispan.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@MBean
@Scope(Scopes.GLOBAL)
public abstract class ManageableExecutorService<T extends ExecutorService> {

   // volatile so reads don't have to be in a synchronized block
   protected volatile T executor;

   @ManagedAttribute(
         description = "Returns the number of threads in this executor.",
         displayName = "Number of executor threads",
         dataType = DataType.TRAIT
   )
   public int getPoolSize() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getPoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the number of active executor threads.",
         displayName = "Number of active executor threads",
         dataType = DataType.TRAIT
   )
   public int getActiveCount() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getActiveCount();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the maximum number of executor threads.",
         displayName = "Maximum number of executor threads",
         dataType = DataType.TRAIT,
         writable = true
   )
   public int getMaximumPoolSize() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getMaximumPoolSize();
      } else {
         return -1;
      }
   }

   public void setMaximumPoolSize(int maximumPoolSize) {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         tp.setMaximumPoolSize(maximumPoolSize);
         if (!(tp.getQueue() instanceof SynchronousQueue)) {
            tp.setCorePoolSize(maximumPoolSize);
         }
      } else {
         throw new UnsupportedOperationException();
      }
   }

   @ManagedAttribute(
         description = "Returns the largest ever number of executor threads.",
         displayName = "Largest number of executor threads",
         dataType = DataType.TRAIT
   )
   public int getLargestPoolSize() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getLargestPoolSize();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the number of elements in this executor's queue.",
         displayName = "Elements in the queue",
         dataType = DataType.TRAIT
   )
   public int getQueueSize() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getQueue().size();
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Returns the keep-alive time for this pool's threads",
         displayName = "Keep-alive for pooled threads",
         dataType = DataType.TRAIT
   )
   public long getKeepAliveTime() {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         return tp.getKeepAliveTime(TimeUnit.MILLISECONDS);
      } else {
         return -1;
      }
   }

   public void setKeepAliveTime(long milliseconds) {
      T unwrapped = unwrapExecutor(executor);
      if (unwrapped instanceof ThreadPoolExecutor tp) {
         tp.setKeepAliveTime(milliseconds, TimeUnit.MILLISECONDS);
      } else {
         throw new UnsupportedOperationException();
      }
   }

   /**
    * Unwraps executor service wrappers to access the underlying ThreadPoolExecutor.
    * Handles multiple layers of wrapping by unwrapping in a loop until the base executor is found.
    */
   @SuppressWarnings("unchecked")
   private T unwrapExecutor(T executor) {
      if (executor == null) {
         return null;
      }
      ExecutorService current = executor;
      while (current instanceof WrappedExecutorService) {
         current = ((WrappedExecutorService) current).unwrap();
      }
      return (T) current;
   }
}
