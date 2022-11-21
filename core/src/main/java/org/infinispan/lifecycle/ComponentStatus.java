package org.infinispan.lifecycle;

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

   public boolean needToDestroyFailedCache() {
      return this == ComponentStatus.FAILED;
   }

   public boolean startAllowed() {
      return this == ComponentStatus.INSTANTIATED;
   }

   public boolean needToInitializeBeforeStart() {
      return this == ComponentStatus.TERMINATED;
   }

   public boolean stopAllowed() {
      switch (this) {
         case INSTANTIATED:
         case TERMINATED:
         case STOPPING:
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
