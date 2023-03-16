package org.infinispan.query.dsl.embedded.impl;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.MetadataProjectable;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

public class ObjectMetadataAdapter implements MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String>,
      MetadataProjectable {

   private final MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> baseAdapter;
   private final AdvancedCache<?, ?> cache;

   public ObjectMetadataAdapter(MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> baseAdapter,
                                AdvancedCache<?, ?> cache) {
      this.baseAdapter = baseAdapter;
      this.cache = cache;
   }

   @Override
   public String getTypeName() {
      return baseAdapter.getTypeName();
   }

   @Override
   public Class<?> getTypeMetadata() {
      return baseAdapter.getTypeMetadata();
   }

   @Override
   public List<String> mapPropertyNamePathToFieldIdPath(String[] path) {
      return baseAdapter.mapPropertyNamePathToFieldIdPath(path);
   }

   @Override
   public ReflectionHelper.PropertyAccessor makeChildAttributeMetadata(ReflectionHelper.PropertyAccessor parentAttributeMetadata, String attribute) {
      return baseAdapter.makeChildAttributeMetadata(parentAttributeMetadata, attribute);
   }

   @Override
   public boolean isComparableProperty(ReflectionHelper.PropertyAccessor propertyAccessor) {
      return baseAdapter.isComparableProperty(propertyAccessor);
   }

   @Override
   public Object projection(Object key, String attribute) {
      CacheEntry<?, ?> cacheEntry = cache.getCacheEntry(key);
      if (cacheEntry == null) {
         return null;
      }

      if (HibernateSearchPropertyHelper.VERSION.equals(attribute)) {
         return cacheEntry.getMetadata().version();
      }
      return null;
   }
}
