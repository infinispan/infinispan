package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TakeOfflineConfiguration.AFTER_FAILURES;
import static org.infinispan.configuration.cache.TakeOfflineConfiguration.MIN_TIME_TO_WAIT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Mircea Markus
 * @see <a href="https://infinispan.org/docs/stable/titles/xsite/xsite.html#taking_a_site_offline">Infinispan Cross-Site documentation</a>
 * @since 5.2
 */
public class TakeOfflineConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TakeOfflineConfiguration> {

   private final AttributeSet attributes;
   private final BackupConfigurationBuilder backupConfigurationBuilder;

   public TakeOfflineConfigurationBuilder(ConfigurationBuilder builder, BackupConfigurationBuilder backupConfigurationBuilder) {
      super(builder);
      attributes = TakeOfflineConfiguration.attributeDefinitionSet();
      this.backupConfigurationBuilder = backupConfigurationBuilder;
   }

   /**
    * The minimal number of milliseconds to wait before taking this site offline. It defaults to 0 (zero).
    * <p>
    * A zero or negative value will disable any waiting time and use only {@link #afterFailures(int)}.
    * <p>
    * The switch to offline status happens after a failed request (times-out or network failure) and {@code
    * minTimeToWait} is already elapsed.
    * <p>
    * When a request fails (after a successful request) the timer is set and it is stopped and reset after a successful
    * request.
    * <p>
    * Check the <a href="https://infinispan.org/docs/stable/titles/xsite/xsite.html#taking_a_site_offline">Infinispan
    * Cross-Site documentation</a> for more information about {@link #minTimeToWait(long)} and {@link
    * #afterFailures(int)}.
    */
   public TakeOfflineConfigurationBuilder minTimeToWait(long minTimeToWait) {
      attributes.attribute(MIN_TIME_TO_WAIT).set(minTimeToWait);
      return this;
   }

   /**
    * The number of consecutive failed request operations after which this site should be taken offline. It default to 0
    * (zero).
    * <p>
    * A zero or negative value will ignore the number of failures and use only {@link #minTimeToWait(long)}.
    * <p>
    * Check the <a href="https://infinispan.org/docs/stable/titles/xsite/xsite.html#taking_a_site_offline">Infinispan
    * Cross-Site documentation</a> for more information about {@link #minTimeToWait(long)} and {@link
    * #afterFailures(int)}.
    */
   public TakeOfflineConfigurationBuilder afterFailures(int afterFailures) {
      attributes.attribute(AFTER_FAILURES).set(afterFailures);
      return this;
   }

   public BackupConfigurationBuilder backup() {
      return backupConfigurationBuilder;
   }

   @Override
   public TakeOfflineConfiguration create() {
      return new TakeOfflineConfiguration(attributes.protect());
   }

   @Override
   public TakeOfflineConfigurationBuilder read(TakeOfflineConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
