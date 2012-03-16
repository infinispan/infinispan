package org.infinispan;

public class ClassLoaderSpecfiedCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> implements AdvancedCache<K, V> {

   private final ClassLoader classLoader;
   
   public ClassLoaderSpecfiedCache(AdvancedCache<K, V> cache, ClassLoader classLoader) {
      super(cache);
      this.classLoader = classLoader;
   }
   
   @Override
   public ClassLoader getClassLoader() {
      return classLoader;
   }

}
