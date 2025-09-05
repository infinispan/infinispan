package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.metadata.Metadata;
import org.infinispan.query.objectfilter.impl.MetadataAdapter;
import org.infinispan.query.objectfilter.impl.util.ReflectionHelper;

public class ReflectionMetadataProjectableAdapter extends MetadataProjectableAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> {

   public ReflectionMetadataProjectableAdapter(MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> baseAdapter) {
      super(baseAdapter);
   }

   @Override
   public boolean isValueProjection(String attribute) {
      return HibernateSearchPropertyHelper.VALUE.equals(attribute);
   }

   @Override
   public Object valueProjection(Object rawValue) {
      return rawValue;
   }

   @Override
   public Object metadataProjection(Metadata metadata, String attribute) {
      if ( HibernateSearchPropertyHelper.VERSION.equals(attribute) ) {
         return metadata.version();
      }
      return null;
   }
}
