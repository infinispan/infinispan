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

   public XSiteStateTransferConfiguration(int chunkSize, long timeout) {
      this.chunkSize = chunkSize;
      this.timeout = timeout;
   }

   public int chunkSize() {
      return chunkSize;
   }

   public long timeout() {
      return timeout;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfiguration{" +
            "chunkSize=" + chunkSize +
            ", timeout=" + timeout +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteStateTransferConfiguration that = (XSiteStateTransferConfiguration) o;

      return chunkSize == that.chunkSize &&
            timeout == that.timeout;

   }

   @Override
   public int hashCode() {
      int result = chunkSize;
      result = 31 * result + (int) (timeout ^ (timeout >>> 32));
      return result;
   }
}
