package org.infinispan.commons.api.functional;

//TODO: Component status is in core/, not commons :(

/**
 * Different states a component may be in.
 *
 * @since 8.0
 */
public enum Status {
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
      return this == Status.FAILED;
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
      return this == Status.RUNNING;
   }

   public boolean startingUp() {
      return this == Status.INITIALIZING || this == Status.INSTANTIATED;
   }

   public boolean isTerminated() {
      return this == Status.TERMINATED;
   }

   public boolean isStopping() {
      return this == Status.STOPPING;
   }

}
