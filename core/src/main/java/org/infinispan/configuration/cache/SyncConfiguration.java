package org.infinispan.configuration.cache;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 */
public class SyncConfiguration {

   private long replTimeout;


   SyncConfiguration(long replTimeout) {
      this.replTimeout = replTimeout;
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public long replTimeout() {
      return replTimeout;
   }
   
   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public SyncConfiguration replTimeout(long l) {
      this.replTimeout = l;
      return this;
   }

   @Override
   public String toString() {
      return "SyncConfiguration{" +
            "replTimeout=" + replTimeout +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SyncConfiguration that = (SyncConfiguration) o;

      if (replTimeout != that.replTimeout) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return (int) (replTimeout ^ (replTimeout >>> 32));
   }

}
