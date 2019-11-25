package org.infinispan.server.context;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Context;
import javax.naming.spi.InitialContextFactory;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class ServerInitialContextFactory implements InitialContextFactory {

   private final ConcurrentMap<String, Object> namedObjects;

   public ServerInitialContextFactory(ConcurrentMap<String, Object> namedObjects) {
      this.namedObjects = namedObjects;
   }

   @Override
   public Context getInitialContext(Hashtable<?, ?> environment) {
      return new ServerInitialContext(namedObjects);
   }
}
