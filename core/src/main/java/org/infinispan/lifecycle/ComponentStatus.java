package org.infinispan.lifecycle;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Different states a component may be in.
 *
 * @author Manik Surtani
 * @see org.infinispan.commons.api.Lifecycle
 * @since 4.0
 */
public enum ComponentStatus {
   /**
    * Object has been instantiated, but start() has not been called.
    */
   INSTANTIATED,
   /**
    * The <code>start()</code> method has been called but not yet completed.
    */
   INITIALIZING,
   /**
    * The <code>start()</code> method has been completed and the component is running.
    */
   RUNNING,
   /**
    * The <code>stop()</code> method has been called but has not yet completed.
    */
   STOPPING,
   /**
    * The <code>stop()</code> method has completed and the component has terminated.
    */
   TERMINATED,
   /**
    * The component is in a failed state due to a problem with one of the other lifecycle transition phases.
    */
   FAILED;

   private static final Log log = LogFactory.getLog(ComponentStatus.class);

   public boolean needToDestroyFailedCache() {
      return this == ComponentStatus.FAILED;
   }

   public boolean startAllowed() {
      switch (this) {
         case INSTANTIATED:
            return true;
         default:
            return false;
      }
   }

   public boolean needToInitializeBeforeStart() {
      switch (this) {
         case TERMINATED:
            return true;
         default:
            return false;
      }
   }

   public boolean stopAllowed() {
      switch (this) {
         case INSTANTIATED:
         case TERMINATED:
         case STOPPING:
         case INITIALIZING:
            return false;
         default:
            return true;
      }

   }

   public boolean allowInvocations() {
      return this == ComponentStatus.RUNNING;
   }

   public boolean startingUp() {
      return this == ComponentStatus.INITIALIZING || this == ComponentStatus.INSTANTIATED;
   }

   public boolean isTerminated() {
      return this == ComponentStatus.TERMINATED;
   }

   public boolean isStopping() {
      return this == ComponentStatus.STOPPING;
   }

}
