package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.infinispan.client.hotrod.configuration.ConnectionPoolConfiguration;

/**
 * Create a Pool based on configuration.
 *
 * @author Mircea.Markus@jboss.com
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public class PropsKeyedObjectPoolFactory<K, V> {

   private final KeyedPooledObjectFactory<K, V> factory;
   private final ConnectionPoolConfiguration configuration;

   public PropsKeyedObjectPoolFactory(KeyedPooledObjectFactory<K, V> factory, ConnectionPoolConfiguration configuration) {
      this.factory = factory;
      this.configuration = configuration;
   }

   public GenericKeyedObjectPool<K, V> createPool() {
      GenericKeyedObjectPoolConfig config = new GenericKeyedObjectPoolConfig();
      config.setMaxTotal(configuration.maxTotal());
      config.setMaxIdlePerKey(configuration.maxIdle());
      config.setMinIdlePerKey(configuration.minIdle());
      config.setNumTestsPerEvictionRun(configuration.numTestsPerEvictionRun());
      config.setMinEvictableIdleTimeMillis(configuration.minEvictableIdleTime());
      config.setTimeBetweenEvictionRunsMillis(configuration.timeBetweenEvictionRuns());
      config.setLifo(configuration.lifo());
      config.setTestOnBorrow(configuration.testOnBorrow());
      config.setTestOnReturn(configuration.testOnReturn());
      config.setTestWhileIdle(configuration.testWhileIdle());

      return new GenericKeyedObjectPool(factory, config);
   }
}
