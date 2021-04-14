package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.CHUNK_SIZE;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.MAX_RETRIES;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.MODE;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.TIMEOUT;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.WAIT_TIME;
import static org.infinispan.util.logging.Log.CONFIG;

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
      int chunkSize = attributes.attribute(StateTransferConfiguration.CHUNK_SIZE).get();
      if (chunkSize <= 0) {
         throw CONFIG.invalidChunkSize(chunkSize);
      }
      if (attributes.attribute(TIMEOUT).get() <= 0) {
         throw CONFIG.invalidXSiteStateTransferTimeout();
      }
      if (attributes.attribute(WAIT_TIME).get() <= 0) {
         throw CONFIG.invalidXSiteStateTransferWaitTime();
      }
      XSiteStateTransferMode mode = attributes.attribute(MODE).get();
      if (mode == null) {
         throw CONFIG.invalidXSiteStateTransferMode();
      }
      if (mode == XSiteStateTransferMode.AUTO && backupConfigurationBuilder.strategy() == BackupConfiguration.BackupStrategy.SYNC) {
         throw CONFIG.autoXSiteStateTransferModeNotAvailableInSync();
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
    * The maximum number of retries when a push state command fails. A value &le; 0 (zero) means that the command does
    * not retry. Default value is 30.
    */
   public final XSiteStateTransferConfigurationBuilder maxRetries(int maxRetries) {
      attributes.attribute(MAX_RETRIES).set(maxRetries);
      return this;
   }

   /**
    * The wait time, in milliseconds, between each retry. The value should be &gt; 0 (zero). Default value is 2
    * seconds.
    */
   public final XSiteStateTransferConfigurationBuilder waitTime(long waitingTimeBetweenRetries) {
      attributes.attribute(WAIT_TIME).set(waitingTimeBetweenRetries);
      return this;
   }

   /**
    * The cross-site state transfer mode.
    * <p>
    * If set to {@link XSiteStateTransferMode#AUTO}, Infinispan automatically starts state transfer when it detects
    * a new view for a backup location that was previously offline.
    */
   public final XSiteStateTransferConfigurationBuilder mode(XSiteStateTransferMode mode) {
      attributes.attribute(MODE).set(mode);
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

   public String toString() {
      return "XSiteStateTransferConfigurationBuilder [attributes=" + attributes + "]";
   }

}
