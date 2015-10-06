package org.infinispan.jmx;

/**
 * Allow the old RPC interceptors to implement the {@link JmxStatisticsExposer} interface by delegating to
 * their sequential counterparts.
 *
 * @author Dan Berindei
 * @since 8.1
 */
public interface DelegatingJmxStatisticsExposer extends JmxStatisticsExposer {
   JmxStatisticsExposer getDelegate();

   @Override
   default boolean getStatisticsEnabled() {
      return getDelegate().getStatisticsEnabled();
   }

   @Override
   default void setStatisticsEnabled(boolean enabled) {
      getDelegate().setStatisticsEnabled(enabled);
   }

   @Override
   default void resetStatistics() {
      getDelegate().resetStatistics();
   }
}
