package org.infinispan.commons.marshall.jboss;

import org.jboss.marshalling.ContextClassResolver;

import java.lang.ref.WeakReference;

/**
 * This class refines <code>ContextClassLoader</code> to add a default class loader
 * in case the context class loader is <code>null</code>.
 *
 * @author Dan Berindei <dberinde@redhat.com>
 * @since 4.2
 */
public class DefaultContextClassResolver extends ContextClassResolver {

   private WeakReference<ClassLoader> defaultClassLoader;

   public DefaultContextClassResolver(ClassLoader defaultClassLoader) {
      this.defaultClassLoader = new WeakReference<ClassLoader>(defaultClassLoader);
   }

   @Override
   protected ClassLoader getClassLoader() {
      ClassLoader loader = super.getClassLoader();
      return loader != null ? loader : defaultClassLoader.get();
   }
}
