package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.infinispan.client.hotrod.configuration.ConnectionPoolConfiguration;
import org.infinispan.client.hotrod.configuration.ExhaustedAction;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class PropsKeyedObjectPoolFactory<K, V> extends GenericKeyedObjectPoolFactory<K, V> {

   public PropsKeyedObjectPoolFactory(KeyedPoolableObjectFactory<K, V> factory, ConnectionPoolConfiguration configuration) {
      super(factory,
            configuration.maxActive(),
            mapExhaustedAction(configuration.exhaustedAction()),
            configuration.maxWait(),
            configuration.maxIdle(),
            configuration.maxTotal(),
            configuration.minIdle(),
            configuration.testOnBorrow(),
            configuration.testOnReturn(),
            configuration.timeBetweenEvictionRuns(),
            configuration.numTestsPerEvictionRun(),
            configuration.minEvictableIdleTime(),
            configuration.testWhileIdle(),
            configuration.lifo());
   }

   private static byte mapExhaustedAction(ExhaustedAction action) {
      switch (action) {
      case CREATE_NEW:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW;
      case EXCEPTION:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL;
      case WAIT:
      default:
         return GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK;
      }
   }
}
