package org.infinispan.configuration.cache;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfiguration;

import java.util.concurrent.TimeUnit;

/**
 * Configuration Builder to configure the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<XSiteStateTransferConfiguration> {

   public static final int DEFAULT_CHUNK_SIZE = 512;
   private int chunkSize = DEFAULT_CHUNK_SIZE;
   public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(20);
   private long timeout = DEFAULT_TIMEOUT;
   public static final int DEFAULT_MAX_RETRIES = 30;
   private int maxRetries = DEFAULT_MAX_RETRIES;
   public static final long DEFAULT_WAIT_TIME = TimeUnit.SECONDS.toMillis(2);
   private long waitTime = DEFAULT_WAIT_TIME;
   private final BackupConfigurationBuilder backupConfigurationBuilder;

   public XSiteStateTransferConfigurationBuilder(ConfigurationBuilder builder,
                                                 BackupConfigurationBuilder backupConfigurationBuilder) {
      super(builder);
      this.backupConfigurationBuilder = backupConfigurationBuilder;
   }

   @Override
   public void validate() {
      if (timeout <= 0) {
         throw new CacheConfigurationException("Timeout must be higher or equals than 1 (one).");
      }
      if (waitTime <= 0) {
         throw new CacheConfigurationException("Waiting time between retries must be higher or equals than 1 (one).");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   /**
    * If &gt; 0, the state will be transferred in batches of {@code chunkSize} cache entries. If &lt;= 0, the state will
    * be transferred in all at once. Not recommended. Defaults to 512.
    */
   public final XSiteStateTransferConfigurationBuilder chunkSize(int chunkSize) {
      this.chunkSize = chunkSize;
      return this;
   }

   /**
    * The time (in milliseconds) to wait for the backup site acknowledge the state chunk received and applied. Default
    * value is 20 min.
    */
   public final XSiteStateTransferConfigurationBuilder timeout(long timeout) {
      this.timeout = timeout;
      return this;
   }

   /**
    * The maximum number of retries when a push state command fails. A value <= 0 (zero) mean that the command will not
    * retry. Default value is 30.
    */
   public final XSiteStateTransferConfigurationBuilder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
   }

   /**
    * The waiting time (in milliseconds) between each retry. The value should be > 0 (zero). Default value is 2 seconds.
    */
   public final XSiteStateTransferConfigurationBuilder waitTime(long waitingTimeBetweenRetries) {
      this.waitTime = waitingTimeBetweenRetries;
      return this;
   }

   public final BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public XSiteStateTransferConfiguration create() {
      return new XSiteStateTransferConfiguration(chunkSize, timeout, maxRetries, waitTime);
   }

   @Override
   public Builder<XSiteStateTransferConfiguration> read(XSiteStateTransferConfiguration template) {
      this.chunkSize = template.chunkSize();
      this.timeout = template.timeout();
      this.maxRetries = template.maxRetries();
      this.waitTime = template.waitTime();
      return this;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfigurationBuilder{" +
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

      XSiteStateTransferConfigurationBuilder that = (XSiteStateTransferConfigurationBuilder) o;

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
