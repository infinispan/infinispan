package org.infinispan.objectfilter.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.ReflectionMatcherEvalContext;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.beans.IntrospectionException;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcher extends BaseMatcher<Class<?>, ReflectionHelper.PropertyAccessor, String> {

   private final EntityNamesResolver entityNamesResolver;

   private final ReflectionPropertyHelper propertyHelper;

   public ReflectionMatcher(ClassLoader classLoader) {
      this(new ReflectionEntityNamesResolver(classLoader));
   }

   protected ReflectionMatcher(EntityNamesResolver entityNamesResolver) {
      if (entityNamesResolver == null) {
         throw new IllegalArgumentException("The EntityNamesResolver argument cannot be null");
      }
      this.entityNamesResolver = entityNamesResolver;
      propertyHelper = new ReflectionPropertyHelper(entityNamesResolver);
   }

   @Override
   protected ReflectionMatcherEvalContext startMultiTypeContext(Object userContext, Object eventType, Object instance) {
      FilterRegistry<Class<?>, ReflectionHelper.PropertyAccessor, String> filterRegistry = getFilterRegistryForType(instance.getClass());
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
   protected FilterProcessingChain<Class<?>> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(entityNamesResolver, propertyHelper, namedParameters);
   }

   @Override
   protected FilterRegistry<Class<?>, ReflectionHelper.PropertyAccessor, String> getFilterRegistryForType(Class<?> entityType) {
      return filtersByType.get(entityType);
   }

   @Override
   public ReflectionPropertyHelper getPropertyHelper() {
      return propertyHelper;
   }

   @Override
   protected MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> createMetadataAdapter(Class<?> clazz) {
      return new MetadataAdapterImpl(clazz);
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<Class<?>, ReflectionHelper.PropertyAccessor, String> {

      private final Class<?> clazz;

      MetadataAdapterImpl(Class<?> clazz) {
         this.clazz = clazz;
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
      public List<String> translatePropertyPath(List<String> path) {
         return path;
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
