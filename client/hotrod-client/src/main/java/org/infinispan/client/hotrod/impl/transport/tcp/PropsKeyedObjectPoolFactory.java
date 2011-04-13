package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPoolFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class PropsKeyedObjectPoolFactory extends GenericKeyedObjectPoolFactory {


   private static final Log log = LogFactory.getLog(PropsKeyedObjectPoolFactory.class);

   public PropsKeyedObjectPoolFactory(KeyedPoolableObjectFactory factory, Properties props) {
      super(factory);
      _maxActive = intProp(props, "maxActive", -1);
      _maxTotal = intProp(props, "maxTotal", -1);
      _maxIdle = intProp(props, "maxIdle", -1);
      _whenExhaustedAction = (byte) intProp(props, "whenExhaustedAction", (int) GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
      _testOnBorrow = booleanProp(props, "testOnBorrow", false);
      _testOnReturn = booleanProp(props, "testOnReturn", false);
      _timeBetweenEvictionRunsMillis = intProp(props, "timeBetweenEvictionRunsMillis", 2 * 60 * 1000);
      _minEvictableIdleTimeMillis = longProp(props, "minEvictableIdleTimeMillis", 5 * 60 * 1000);
      _numTestsPerEvictionRun = intProp(props, "numTestsPerEvictionRun", 3);
      _testWhileIdle = booleanProp(props, "testWhileIdle", true);
      _minIdle = intProp(props, "minIdle", 1);
      _lifo = booleanProp(props, "lifo", true);
   }

   private int intProp(Properties p, String name, int defaultValue) {
      return (Integer) getValue(p, name, defaultValue);
   }

   private boolean booleanProp(Properties p, String name, Boolean defaultValue) {
      return (Boolean) getValue(p, name, defaultValue);
   }

   private long longProp(Properties p, String name, long defaultValue) {
      return (Long) getValue(p, name, defaultValue);
   }

   public Object getValue(Properties p, String name, Object defaultValue) {
      Object propValue = p.get(name);
      if (propValue == null) {
         if (log.isTraceEnabled()) {
            log.trace(name + " property not specified, using default value(" + defaultValue + ")");
         }
         return defaultValue;
      } else {
         log.trace(name + " = " + propValue);
         if (defaultValue instanceof Integer) {
            return Integer.parseInt(propValue.toString());
         } else if (defaultValue instanceof Boolean) {
            return Boolean.parseBoolean(propValue.toString());
         } else if (defaultValue instanceof Long) {
            return Long.parseLong(propValue.toString());
         } else {
            throw new IllegalStateException();
         }
      }
   }
}

