package org.infinispan.test.hibernate.cache.commons.naming;

import java.util.Hashtable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.naming.Context;
import javax.naming.spi.InitialContextFactory;

public class TrivialInitialContextFactory implements InitialContextFactory {

   private final ConcurrentMap<String, Object> namedObjects;
   static final ConcurrentMap<String, Object> SHARED = new ConcurrentHashMap<>();

   public TrivialInitialContextFactory() {
      namedObjects = SHARED;
   }

   public TrivialInitialContextFactory(ConcurrentMap<String, Object> namedObjects) {
      this.namedObjects = namedObjects;
   }

   @Override
   public Context getInitialContext(Hashtable<?, ?> environment) {
      return new TrivialInitialContext(namedObjects);
   }
}
