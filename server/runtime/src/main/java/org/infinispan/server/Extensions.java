package org.infinispan.server;

import java.sql.Driver;
import java.util.HashMap;
import java.util.Map;

import javax.script.ScriptEngineFactory;

import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.filter.KeyValueFilterConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.filter.ParamKeyValueFilterConverterFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.tasks.ServerTaskEngine;
import org.infinispan.server.tasks.ServerTaskWrapper;
import org.infinispan.tasks.ServerTask;
import org.infinispan.tasks.spi.TaskEngine;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Extensions {
   private final Map<String, CacheEventFilterFactory> filterFactories = new HashMap<>();
   private final Map<String, CacheEventConverterFactory> converterFactories = new HashMap<>();
   private final Map<String, CacheEventFilterConverterFactory> filterConverterFactories = new HashMap<>();
   private final Map<String, KeyValueFilterConverterFactory> keyValueFilterConverterFactories = new HashMap<>();
   private final Map<String, ParamKeyValueFilterConverterFactory> paramKeyValueFilterConverterFactories = new HashMap<>();
   private final Map<String, ServerTaskWrapper> serverTasks = new HashMap<>();

   public Extensions() {
   }

   public void load(ClassLoader classLoader) {
      loadNamedFactory(classLoader, CacheEventFilterFactory.class, filterFactories);
      loadNamedFactory(classLoader, CacheEventConverterFactory.class, converterFactories);
      loadNamedFactory(classLoader, CacheEventFilterConverterFactory.class, filterConverterFactories);
      loadNamedFactory(classLoader, KeyValueFilterConverterFactory.class, keyValueFilterConverterFactories);
      loadNamedFactory(classLoader, ParamKeyValueFilterConverterFactory.class, paramKeyValueFilterConverterFactories);
      loadService(classLoader, Driver.class);
      if (loadService(classLoader, ScriptEngineFactory.class) == 0) {
         Server.log.noScriptEngines();
      }
      loadServerTasks(classLoader);
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
      for (Map.Entry<String, ParamKeyValueFilterConverterFactory> factory : paramKeyValueFilterConverterFactories.entrySet()) {
         server.addKeyValueFilterConverterFactory(factory.getKey(), factory.getValue());
      }
   }

   private <T> void loadNamedFactory(ClassLoader classLoader, Class<T> contract, Map<String, T> map) {
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

   private <T> int loadService(ClassLoader classLoader, Class<T> contract) {
      int i = 0;
      for (T t : ServiceFinder.load(contract, classLoader)) {
         Server.log.loadedExtension(t.getClass().getName());
         i++;
      }
      return i;
   }

   private void loadServerTasks(ClassLoader classLoader) {
      for (ServerTask t : ServiceFinder.load(ServerTask.class, classLoader)) {
         serverTasks.put(t.getName(), new ServerTaskWrapper(t));
         Server.log.loadedExtension(t.getClass().getName());
      }
   }

   public TaskEngine getServerTaskEngine(EmbeddedCacheManager cm) {
      return new ServerTaskEngine(cm, serverTasks);
   }
}
