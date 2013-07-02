package org.infinispan.configuration.cache;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfiguration {

   private int afterFailures;
   private long minTimeToWait;

   public TakeOfflineConfiguration(int afterFailures, long minTimeToWait) {
      this.afterFailures = afterFailures;
      this.minTimeToWait = minTimeToWait;
   }

   /**
    * @see TakeOfflineConfigurationBuilder#afterFailures(int)
    */
   public int afterFailures() {
      return afterFailures;
   }

   /**
    * @see TakeOfflineConfigurationBuilder#minTimeToWait(long)
    */
   public long minTimeToWait() {
      return minTimeToWait;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TakeOfflineConfiguration)) return false;

      TakeOfflineConfiguration that = (TakeOfflineConfiguration) o;

      if (afterFailures != that.afterFailures) return false;
      if (minTimeToWait != that.minTimeToWait) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = afterFailures;
      result = 31 * result + (int) (minTimeToWait ^ (minTimeToWait >>> 32));
      return result;
   }

   public boolean enabled() {
      return minTimeToWait > 0 || afterFailures > 0;
   }

   @Override
   public String toString() {
      return "TakeOfflineConfiguration{" +
            "afterFailures=" + afterFailures +
            ", minTimeToWait=" + minTimeToWait +
            '}';
   }
}
