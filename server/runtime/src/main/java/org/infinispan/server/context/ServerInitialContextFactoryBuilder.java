package org.infinispan.server.context;

import java.io.Closeable;
import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.spi.InitialContextFactory;
import javax.naming.spi.InitialContextFactoryBuilder;

import org.infinispan.commons.util.Util;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ServerInitialContextFactoryBuilder implements InitialContextFactoryBuilder, Closeable {

   private final ConcurrentMap<String, Object> NAMED_OBJECTS;

   public ServerInitialContextFactoryBuilder() {
      NAMED_OBJECTS = new ConcurrentHashMap<>();
   }

   @Override
   public InitialContextFactory createInitialContextFactory(Hashtable<?, ?> environment) throws NamingException {
      String className = environment != null ? (String) environment.get(Context.INITIAL_CONTEXT_FACTORY) : null;
      if (className == null) {
         return new ServerInitialContextFactory(NAMED_OBJECTS);
      }
      return InitialContextFactory.class.cast(Util.getInstance(className, Thread.currentThread().getContextClassLoader()));
   }

   @Override
   public void close() {
      // Closes any AutoCloseable objects stored in the context
      NAMED_OBJECTS.values().stream().filter(v -> v instanceof AutoCloseable).forEach(o -> Util.close((AutoCloseable) o));
   }
}
