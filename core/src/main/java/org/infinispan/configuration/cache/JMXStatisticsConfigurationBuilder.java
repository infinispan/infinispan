package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;

/**
 * Determines whether cache statistics are gathered.
 *
 * @author pmuir
 * @deprecated since 10.1.3. Use {@link StatisticsConfigurationBuilder} instead. This will be removed in next major
 * version.
 */
@Deprecated
public abstract class JMXStatisticsConfigurationBuilder extends AbstractConfigurationChildBuilder implements ConfigurationBuilderInfo {

   JMXStatisticsConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }

   /**
    * Enable statistics gathering and reporting
    */
   public abstract StatisticsConfigurationBuilder enable();

   /**
    * Disable statistics gathering and reporting
    */
   public abstract StatisticsConfigurationBuilder disable();

   /**
    * Enable or disable statistics gathering and reporting
    */
   public abstract StatisticsConfigurationBuilder enabled(boolean enabled);

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    *
    * @deprecated since 10.1.3. This method will be removed in a future version.
    */
   @Deprecated
   public abstract StatisticsConfigurationBuilder available(boolean available);
}
