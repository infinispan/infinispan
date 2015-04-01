package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TakeOfflineConfiguration.AFTER_FAILURES;
import static org.infinispan.configuration.cache.TakeOfflineConfiguration.MIN_TIME_TO_WAIT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TakeOfflineConfiguration>{


   private final AttributeSet attributes;
   private BackupConfigurationBuilder backupConfigurationBuilder;

   public TakeOfflineConfigurationBuilder(ConfigurationBuilder builder, BackupConfigurationBuilder backupConfigurationBuilder) {
      super(builder);
      this.attributes = TakeOfflineConfiguration.attributeDefinitionSet();
      this.backupConfigurationBuilder = backupConfigurationBuilder;
   }

   /**
    * The minimal number of millis to wait before taking this site offline, even in the case 'afterFailures' is reached.
    * If smaller or equal to 0, then only 'afterFailures' is considered.
    */
   public TakeOfflineConfigurationBuilder minTimeToWait(long minTimeToWait) {
      attributes.attribute(MIN_TIME_TO_WAIT).set(minTimeToWait);
      return this;
   }

   /**
    * The number of failed request operations after which this site should be taken offline. Defaults to 0 (never). A
    * negative value would mean that the site will be taken offline after 'minTimeToWait'.
    */
   public TakeOfflineConfigurationBuilder afterFailures(int afterFailures) {
      attributes.attribute(AFTER_FAILURES).set(afterFailures);
      return this;
   }

   public BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public TakeOfflineConfiguration create() {
      return new TakeOfflineConfiguration(attributes.protect());
   }

   @Override
   public TakeOfflineConfigurationBuilder read(TakeOfflineConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }
}
