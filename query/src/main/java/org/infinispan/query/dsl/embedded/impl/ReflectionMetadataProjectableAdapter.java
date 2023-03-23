package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

public class ReflectionMetadataProjectableAdapter extends MetadataProjectableAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> {

   public ReflectionMetadataProjectableAdapter(MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> baseAdapter, AdvancedCache<?, ?> cache) {
      super(baseAdapter, cache);
   }

   @Override
   public Object projection(CacheEntry<?, ?> cacheEntry, String attribute) {
      if (HibernateSearchPropertyHelper.VALUE.equals(attribute)) {
         return cacheEntry.getValue();
      }

      Metadata metadata = cacheEntry.getMetadata();
      if (HibernateSearchPropertyHelper.VERSION.equals(attribute)) {
         return metadata.version();
      }
      return null;
   }
}
