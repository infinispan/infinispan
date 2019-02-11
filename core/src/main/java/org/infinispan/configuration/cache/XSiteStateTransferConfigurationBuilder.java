package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.CHUNK_SIZE;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.ELEMENT_DEFINITION;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.MAX_RETRIES;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.TIMEOUT;
import static org.infinispan.configuration.cache.XSiteStateTransferConfiguration.WAIT_TIME;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Configuration Builder to configure the state transfer between sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfigurationBuilder extends AbstractConfigurationChildBuilder
      implements Builder<XSiteStateTransferConfiguration>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(XSiteStateTransferConfigurationBuilder.class);
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
         throw log.invalidXSiteStateTransferTimeout();
      }
      if (attributes.attribute(WAIT_TIME).get() <= 0) {
         throw log.invalidXSiteStateTransferWaitTime();
      }
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
    * The maximum number of retries when a push state command fails. A value &le; 0 (zero) mean that the command will not
    * retry. Default value is 30.
    */
   public final XSiteStateTransferConfigurationBuilder maxRetries(int maxRetries) {
      attributes.attribute(MAX_RETRIES).set(maxRetries);
      return this;
   }

   /**
    * The waiting time (in milliseconds) between each retry. The value should be &gt; 0 (zero). Default value is 2 seconds.
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
