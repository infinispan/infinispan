package org.infinispan.api;

import java.net.URI;
import java.util.Iterator;
import java.util.ServiceLoader;

import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.configuration.Configuration;
import org.infinispan.api.exception.InfinispanConfigurationException;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.sync.SyncContainer;

/**
 * Infinispan instance, embedded or client, depending on the access point.
 *
 * @since 14.0
 */
@Experimental("This is not ready yet for general consumption. Major changes are still expected.")
public interface Infinispan extends AutoCloseable {
   /**
    * Creates and starts the Infinispan manager object
    *
    * Calling create with the same URI multiple times should return the same object. Ref count the object.
    *
    * <ul>
    *   <li><tt>file:///.../infinispan.xml</tt> Embedded Infinispan configured via XML/JSON/Yaml file</li>
    *   <li><tt>classpath:///.../infinispan.xml</tt> Embedded Infinispan configured via XML/JSON/Yaml classpath resource</li>
    *   <li><tt>hotrod[s]://[username[:password]@]host:port[,host2:port]?property=value[&property=value]</tt> </li>
    * </ul>
    *
    * @param uri one of the supported Infinispan URIs:
    * @return an
    */
   static Infinispan create(URI uri) {
      Iterator<Factory> factories = ServiceLoader.load(Factory.class, Factory.class.getClassLoader()).iterator();
      while (factories.hasNext()) {
         Factory factory = factories.next();
         Infinispan instance = factory.create(uri);
         if (instance != null) {
            return instance;
         }
      }
      throw new InfinispanConfigurationException("No factory to handle URI " + uri);
   }

   static Infinispan create(String uri) {
      return create(URI.create(uri));
   }

   static Infinispan create(Configuration configuration) {
      Iterator<Factory> factories = ServiceLoader.load(Factory.class, Factory.class.getClassLoader()).iterator();
      while (factories.hasNext()) {
         Factory factory = factories.next();
         Infinispan instance = factory.create(configuration);
         if (instance != null) {
            return instance;
         }
      }
      throw new InfinispanConfigurationException("No factory to handle configuration " + configuration);
   }

   /**
    * Returns a synchronous version of the Infinispan API
    *
    * @return
    */
   SyncContainer sync();

   /**
    * Returns an asynchronous version of the Infinispan API
    *
    * @return
    */
   AsyncContainer async();

   /**
    * Returns a mutiny version of the Infinispan API
    *
    * @return
    */
   MutinyContainer mutiny();

   /**
    * Closes the instance, releasing all allocated resources (thread pools, open files, etc)
    */
   @Override
   void close();

   interface Factory {
      /**
       * Create an {@link Infinispan} instance for the supplied uri. If the factory cannot handle the uri, it should return null.
       * @param uri
       * @return An {@link Infinispan} instance or null if the factory cannot handle the uri.
       */
      Infinispan create(URI uri);

      /**
       * Create an {@link Infinispan} instance for the supplied configuration. If the factory cannot handle the uri, it should return null.
       * @param configuration
       * @return An {@link Infinispan} instance or null if the factory cannot handle the uri.
       */
      Infinispan create(Configuration configuration);
   }
}
