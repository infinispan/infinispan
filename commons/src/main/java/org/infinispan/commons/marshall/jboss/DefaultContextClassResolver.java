package org.infinispan.commons.marshall.jboss;

import java.lang.ref.WeakReference;

import org.jboss.marshalling.ContextClassResolver;

/**
 * This class refines <code>ContextClassLoader</code> to add a default class loader.
 * The context class loader is only used when the default is <code>null</code>.
 *
 * @author Dan Berindei &lt;dberinde@redhat.com&gt;
 * @since 4.2
 */
public class DefaultContextClassResolver extends ContextClassResolver {

   private WeakReference<ClassLoader> defaultClassLoader;

   public DefaultContextClassResolver(ClassLoader defaultClassLoader) {
      this.defaultClassLoader = new WeakReference<ClassLoader>(defaultClassLoader);
   }

   @Override
   protected ClassLoader getClassLoader() {
      ClassLoader defaultLoader = this.defaultClassLoader.get();
      return defaultLoader != null ? defaultLoader : super.getClassLoader();
   }
}
