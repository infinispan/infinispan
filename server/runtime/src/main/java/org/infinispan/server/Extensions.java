package org.infinispan.server;

import java.sql.Driver;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Extensions {
   private final Map<String, CacheEventFilterFactory> filterFactories = new HashMap<>();
   private final Map<String, CacheEventConverterFactory> converterFactories = new HashMap<>();
   private final Map<String, CacheEventFilterConverterFactory> filterConverterFactories = new HashMap<>();
   private final Map<String, KeyValueFilterConverterFactory> keyValueFilterConverterFactories = new HashMap<>();

   public Extensions() {
   }

   public void load(ClassLoader classLoader) {
      load(classLoader, CacheEventFilterFactory.class, filterFactories);
      load(classLoader, CacheEventConverterFactory.class, converterFactories);
      load(classLoader, CacheEventFilterConverterFactory.class, filterConverterFactories);
      load(classLoader, KeyValueFilterConverterFactory.class, keyValueFilterConverterFactories);
      load(classLoader, Driver.class);
   }

   public void apply(HotRodServer server) {
      for (Map.Entry<String, CacheEventFilterFactory> factory : filterFactories.entrySet()) {
         server.addCacheEventFilterFactory(factory.getKey(), factory.getValue());
      }
      for (Map.Entry<String, CacheEventConverterFactory> factory : converterFactories.entrySet()) {
         server.addCacheEventConverterFactory(factory.getKey(), factory.getValue());
      }
      for (Map.Entry<String, CacheEventFilterConverterFactory> factory : filterConverterFactories.entrySet()) {
         server.addCacheEventFilterConverterFactory(factory.getKey(), factory.getValue());
      }
      for (Map.Entry<String, KeyValueFilterConverterFactory> factory : keyValueFilterConverterFactories.entrySet()) {
         server.addKeyValueFilterConverterFactory(factory.getKey(), factory.getValue());
      }
   }

   private <T> void load(ClassLoader classLoader, Class<T> contract, Map<String, T> map) {
      for (T t : ServiceFinder.load(contract, classLoader)) {
         NamedFactory name = t.getClass().getAnnotation(NamedFactory.class);
         if (name != null) {
            map.put(name.name(), t);
            Server.log.loadedExtension(name.name());
         } else {
            Server.log.unnamedFactoryClass(t.getClass().getName());
         }
      }
   }

   private <T> void load(ClassLoader classLoader, Class<T> contract) {
      for (T t : ServiceFinder.load(contract, classLoader)) {
         Server.log.loadedExtension(t.getClass().getName());
      }
   }
}
