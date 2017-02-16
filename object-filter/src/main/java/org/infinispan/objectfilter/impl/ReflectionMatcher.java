package org.infinispan.objectfilter.impl;

import java.beans.IntrospectionException;
import java.util.List;

import org.infinispan.objectfilter.impl.predicateindex.ReflectionMatcherEvalContext;
import org.infinispan.objectfilter.impl.syntax.parser.EntityNameResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ObjectPropertyHelper;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.syntax.parser.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcher extends BaseMatcher<Class<?>, ReflectionHelper.PropertyAccessor, String> {

   public ReflectionMatcher(ObjectPropertyHelper<Class<?>> propertyHelper) {
      super(propertyHelper);
   }

   public ReflectionMatcher(ClassLoader classLoader) {
      this(new ReflectionEntityNamesResolver(classLoader));
   }

   public ReflectionMatcher(EntityNameResolver entityNameResolver) {
      super(new ReflectionPropertyHelper(entityNameResolver));
   }

   @Override
   protected ReflectionMatcherEvalContext startMultiTypeContext(boolean isDeltaFilter, Object userContext, Object eventType, Object instance) {
      FilterRegistry<Class<?>, ReflectionHelper.PropertyAccessor, String> filterRegistry = getFilterRegistryForType(isDeltaFilter, instance.getClass());
      if (filterRegistry != null) {
         ReflectionMatcherEvalContext context = new ReflectionMatcherEvalContext(userContext, eventType, instance);
         context.initMultiFilterContext(filterRegistry);
         return context;
      }
      return null;
   }

   @Override
   protected ReflectionMatcherEvalContext startSingleTypeContext(Object userContext, Object eventType, Object instance, MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> metadataAdapter) {
      if (metadataAdapter.getTypeMetadata() == instance.getClass()) {
         return new ReflectionMatcherEvalContext(userContext, eventType, instance);
      } else {
         return null;
      }
   }

   @Override
   protected MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> createMetadataAdapter(Class<?> clazz) {
      return new MetadataAdapterImpl(clazz, propertyHelper);
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> {

      private final Class<?> clazz;

      private final ObjectPropertyHelper<Class<?>> propertyHelper;

      MetadataAdapterImpl(Class<?> clazz, ObjectPropertyHelper<Class<?>> propertyHelper) {
         this.clazz = clazz;
         this.propertyHelper = propertyHelper;
      }

      @Override
      public String getTypeName() {
         return clazz.getName();
      }

      @Override
      public Class<?> getTypeMetadata() {
         return clazz;
      }

      @Override
      public List<String> mapPropertyNamePathToFieldIdPath(String[] path) {
         return (List<String>) propertyHelper.mapPropertyNamePathToFieldIdPath(clazz, path);
      }

      @Override
      public ReflectionHelper.PropertyAccessor makeChildAttributeMetadata(ReflectionHelper.PropertyAccessor parentAttributeMetadata, String attribute) {
         try {
            return parentAttributeMetadata == null ?
                  ReflectionHelper.getAccessor(clazz, attribute) : parentAttributeMetadata.getAccessor(attribute);
         } catch (IntrospectionException e) {
            return null;
         }
      }

      @Override
      public boolean isComparableProperty(ReflectionHelper.PropertyAccessor attributeMetadata) {
         Class<?> propertyType = attributeMetadata.getPropertyType();
         return propertyType != null && (propertyType.isPrimitive() || Comparable.class.isAssignableFrom(propertyType));
      }
   }
}
