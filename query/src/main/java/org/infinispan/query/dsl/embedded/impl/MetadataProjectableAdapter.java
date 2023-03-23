package org.infinispan.query.dsl.embedded.impl;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.objectfilter.impl.MetadataAdapter;
import org.infinispan.objectfilter.impl.predicateindex.MetadataProjectable;

abstract public class MetadataProjectableAdapter<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>>
      implements MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId>,
      MetadataProjectable<AttributeId> {

   private final MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> baseAdapter;
   private final AdvancedCache<?, ?> cache;

   public MetadataProjectableAdapter(MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId> baseAdapter,
                                     AdvancedCache<?, ?> cache) {
      this.baseAdapter = baseAdapter;
      this.cache = cache;
   }

   @Override
   public String getTypeName() {
      return baseAdapter.getTypeName();
   }

   @Override
   public TypeMetadata getTypeMetadata() {
      return baseAdapter.getTypeMetadata();
   }

   @Override
   public List<AttributeId> mapPropertyNamePathToFieldIdPath(String[] path) {
      return baseAdapter.mapPropertyNamePathToFieldIdPath(path);
   }

   @Override
   public AttributeMetadata makeChildAttributeMetadata(AttributeMetadata parentAttributeMetadata, AttributeId attribute) {
      return baseAdapter.makeChildAttributeMetadata(parentAttributeMetadata, attribute);
   }

   @Override
   public boolean isComparableProperty(AttributeMetadata propertyAccessor) {
      return baseAdapter.isComparableProperty(propertyAccessor);
   }

   @Override
   public Object projection(Object key, AttributeId attribute) {
      CacheEntry<?, ?> cacheEntry = cache.getCacheEntry(key);
      if (cacheEntry == null) {
         return null;
      }
      return projection(cacheEntry, attribute);
   }

   public abstract Object projection(CacheEntry<?, ?> cacheEntry, AttributeId attribute);

}
