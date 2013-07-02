package org.infinispan.query.classloading;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 */
public class CountingClassLoader extends ClassLoader {

   public final AtomicInteger countInvocations = new AtomicInteger();

   @Override
   public Class<?> loadClass(String name) throws ClassNotFoundException {
      countInvocations.incrementAndGet();
      return super.loadClass(name);
   }

}
