package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.*;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * Configuration Builder to configure the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<XSiteStateTransferConfiguration> {
   public static final int DEFAULT_CHUNK_SIZE = CHUNK_SIZE.getDefaultValue();
   public static final long DEFAULT_TIMEOUT = TIMEOUT.getDefaultValue();
   public static final int DEFAULT_MAX_RETRIES = MAX_RETRIES.getDefaultValue();
   public static final long DEFAULT_WAIT_TIME = WAIT_TIME.getDefaultValue();
   private final BackupConfigurationBuilder backupConfigurationBuilder;
   private final AttributeSet attributes;

   public XSiteStateTransferConfigurationBuilder(ConfigurationBuilder builder,
                                                 BackupConfigurationBuilder backupConfigurationBuilder) {
      super(builder);
      this.attributes = XSiteStateTransferConfiguration.attributeDefinitionSet();
      this.backupConfigurationBuilder = backupConfigurationBuilder;
   }

   @Override
   public void validate() {
      if (attributes.attribute(TIMEOUT).get() <= 0) {
         throw new CacheConfigurationException("Timeout must be higher or equals than 1 (one).");
      }
      if (attributes.attribute(WAIT_TIME).get() <= 0) {
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
      attributes.attribute(CHUNK_SIZE).set(chunkSize);
      return this;
   }

   /**
    * The time (in milliseconds) to wait for the backup site acknowledge the state chunk received and applied. Default
    * value is 20 min.
    */
   public final XSiteStateTransferConfigurationBuilder timeout(long timeout) {
      attributes.attribute(TIMEOUT).set(timeout);
      return this;
   }

   /**
    * The maximum number of retries when a push state command fails. A value <= 0 (zero) mean that the command will not
    * retry. Default value is 30.
    */
   public final XSiteStateTransferConfigurationBuilder maxRetries(int maxRetries) {
      attributes.attribute(MAX_RETRIES).set(maxRetries);
      return this;
   }

   /**
    * The waiting time (in milliseconds) between each retry. The value should be > 0 (zero). Default value is 2 seconds.
    */
   public final XSiteStateTransferConfigurationBuilder waitTime(long waitingTimeBetweenRetries) {
      attributes.attribute(WAIT_TIME).set(waitingTimeBetweenRetries);
      return this;
   }

   public final BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public XSiteStateTransferConfiguration create() {
      return new XSiteStateTransferConfiguration(attributes.protect());
   }

   @Override
   public Builder<XSiteStateTransferConfiguration> read(XSiteStateTransferConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }

}
