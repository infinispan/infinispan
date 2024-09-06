package org.infinispan.query.dsl.embedded.impl;

import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.metadata.Metadata;
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
   public Object projection(Object key, Object instance, Object metadata, AttributeId attribute) {
      if (isValueProjection(attribute)) {
         return valueProjection(instance);
      }

      Metadata meta;
      if (metadata != null) {
         meta = (Metadata) metadata;
      } else {
         CacheEntry<?, ?> cacheEntry = cache.getCacheEntry(key);
         meta = cacheEntry.getMetadata();
      }
      return metadataProjection(meta, attribute);
   }

   public abstract boolean isValueProjection(AttributeId attribute);

   public abstract Object valueProjection(Object rawValue);

   public abstract Object metadataProjection(Metadata metadata, AttributeId attribute);

}
