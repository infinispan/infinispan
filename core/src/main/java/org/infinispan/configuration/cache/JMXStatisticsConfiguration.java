package org.infinispan.configuration.cache;

/**
 * Determines whether cache statistics are gathered.
 *
 * @author pmuir
 * @deprecated since 10.1.3. Use {@link StatisticsConfiguration} instead. This will be removed in next major version.
 */
@Deprecated
public interface JMXStatisticsConfiguration {

   boolean enabled();

   /**
    * If set to false, statistics gathering cannot be enabled during runtime. Performance optimization.
    *
    * @deprecated since 10.1.3. This method will be removed in a future version.
    */
   @Deprecated
   boolean available();
}
