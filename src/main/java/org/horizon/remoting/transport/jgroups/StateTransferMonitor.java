package org.horizon.remoting.transport.jgroups;

import org.horizon.statetransfer.StateTransferException;

public class StateTransferMonitor {
   /**
    * Reference to an exception that was raised during state installation on this cache.
    */
   protected volatile StateTransferException setStateException;
   private final Object stateLock = new Object();
   /**
    * True if state was initialized during start-up.
    */
   private volatile boolean isStateSet = false;

   public StateTransferException getSetStateException() {
      return setStateException;
   }

   public void waitForState() throws Exception {
      synchronized (stateLock) {
         while (!isStateSet) {
            if (setStateException != null) {
               throw setStateException;
            }

            try {
               stateLock.wait();
            }
            catch (InterruptedException iex) {
            }
         }
      }
   }

   public void notifyStateReceiptSucceeded() {
      synchronized (stateLock) {
         isStateSet = true;
         // Notify wait that state has been set.
         stateLock.notifyAll();
      }
   }

   public void notifyStateReceiptFailed(StateTransferException setStateException) {
      this.setStateException = setStateException;
      isStateSet = false;
      notifyStateReceiptSucceeded();
   }
}
