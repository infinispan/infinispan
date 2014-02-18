package org.infinispan.configuration.cache;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;

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

   public final BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public XSiteStateTransferConfiguration create() {
      return new XSiteStateTransferConfiguration(chunkSize, timeout);
   }

   @Override
   public Builder<XSiteStateTransferConfiguration> read(XSiteStateTransferConfiguration template) {
      this.chunkSize = template.chunkSize();
      this.timeout = template.timeout();
      return this;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfigurationBuilder{" +
            "chunkSize=" + chunkSize +
            ", timeout=" + timeout +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteStateTransferConfigurationBuilder that = (XSiteStateTransferConfigurationBuilder) o;

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
