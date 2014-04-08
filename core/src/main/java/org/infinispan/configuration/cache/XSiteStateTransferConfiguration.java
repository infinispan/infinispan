package org.infinispan.configuration.cache;

/**
 * Configuration needed for State Transfer between different sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfiguration {

   private final int chunkSize;
   private final long timeout;
   private final int maxRetries;
   private final long waitTime;

   public XSiteStateTransferConfiguration(int chunkSize, long timeout, int maxRetries, long waitTime) {
      this.chunkSize = chunkSize;
      this.timeout = timeout;
      this.maxRetries = maxRetries;
      this.waitTime = waitTime;
   }

   public int chunkSize() {
      return chunkSize;
   }

   public long timeout() {
      return timeout;
   }

   public int maxRetries() {
      return maxRetries;
   }

   public long waitTime() {
      return waitTime;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfiguration{" +
            "chunkSize=" + chunkSize +
            ", timeout=" + timeout +
            ", maxRetries=" + maxRetries +
            ", waitTime=" + waitTime +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteStateTransferConfiguration that = (XSiteStateTransferConfiguration) o;

      if (chunkSize != that.chunkSize) return false;
      if (maxRetries != that.maxRetries) return false;
      if (timeout != that.timeout) return false;
      if (waitTime != that.waitTime) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = chunkSize;
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      result = 31 * result + maxRetries;
      result = 31 * result + (int) (waitTime ^ (waitTime >>> 32));
      return result;
   }
}
