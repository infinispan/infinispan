package org.infinispan.server.context;

import java.io.Closeable;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.InitialContextFactoryBuilder;

import org.infinispan.commons.util.ServiceFinder;
import org.infinispan.commons.util.Util;


/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ServerInitialContextFactoryBuilder implements InitialContextFactoryBuilder, Closeable {

   private final ConcurrentMap<String, Object> namedObjects;
   private final Map<String, InitialContextFactory> initialContextFactories;

   public ServerInitialContextFactoryBuilder() {
      namedObjects = new ConcurrentHashMap<>();
      initialContextFactories = ServiceFinder.load(InitialContextFactory.class, Thread.currentThread().getContextClassLoader()).stream().collect(Collectors.toMap(className(), Function.identity()));
   }

   @Override
   public InitialContextFactory createInitialContextFactory(Hashtable<?, ?> environment) throws NamingException {
      String className = environment != null ? (String) environment.get(Context.INITIAL_CONTEXT_FACTORY) : null;
      if (className == null) {
         return new ServerInitialContextFactory(namedObjects);
      }
      if (initialContextFactories.containsKey(className)) {
         return initialContextFactories.get(className);
      } else {
         return InitialContextFactory.class.cast(Util.getInstance(className, Thread.currentThread().getContextClassLoader()));
      }
   }

   @Override
   public void close() {
      // Closes any AutoCloseable objects stored in the context
      namedObjects.values().stream().filter(v -> v instanceof AutoCloseable).forEach(o -> Util.close((AutoCloseable) o));
   }

   static <T> Function<T, String> className() {
      return t -> t.getClass().getName();
   }
}
