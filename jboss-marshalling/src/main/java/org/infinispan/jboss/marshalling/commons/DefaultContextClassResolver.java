package org.infinispan.jboss.marshalling.commons;

import java.lang.ref.WeakReference;

import org.jboss.marshalling.ContextClassResolver;

/**
 * This class refines <code>ContextClassLoader</code> to add a default class loader.
 * The context class loader is only used when the default is <code>null</code>.
 *
 * @author Dan Berindei &lt;dberinde@redhat.com&gt;
 * @since 4.2
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11947.
 */
@Deprecated
public class DefaultContextClassResolver extends ContextClassResolver {

   private final WeakReference<ClassLoader> defaultClassLoader;

   public DefaultContextClassResolver(ClassLoader defaultClassLoader) {
      this.defaultClassLoader = new WeakReference<>(defaultClassLoader);
   }

   @Override
   protected ClassLoader getClassLoader() {
      ClassLoader defaultLoader = defaultClassLoader.get();
      return defaultLoader != null ? defaultLoader : super.getClassLoader();
   }
}
