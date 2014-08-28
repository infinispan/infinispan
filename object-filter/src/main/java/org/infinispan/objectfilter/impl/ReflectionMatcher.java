package org.infinispan.objectfilter.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.ReflectionMatcherEvalContext;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

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
   protected ReflectionMatcherEvalContext startContext(Object instance) {
      FilterRegistry<Class<?>, ReflectionHelper.PropertyAccessor, String> filterRegistry = getFilterRegistryForType(instance.getClass());
      if (filterRegistry != null) {
         ReflectionMatcherEvalContext context = createContext(instance);
         context.initMultiFilterContext(filterRegistry);
         return context;
      }
      return null;
   }

   @Override
   protected ReflectionMatcherEvalContext startContext(Object instance, FilterSubscriptionImpl<Class<?>, ReflectionHelper.PropertyAccessor, String> filterSubscription) {
      if (filterSubscription.getMetadataAdapter().getTypeMetadata() == instance.getClass()) {
         return createContext(instance);
      } else {
         return null;
      }
   }

   @Override
   protected ReflectionMatcherEvalContext createContext(Object instance) {
      return new ReflectionMatcherEvalContext(instance);
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
      public boolean isRepeatedProperty(List<String> propertyPath) {
         ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(clazz, propertyPath.get(0));
         if (a.isMultiple()) {
            return true;
         }
         for (int i = 1; i < propertyPath.size(); i++) {
            a = a.getAccessor(propertyPath.get(i));
            if (a.isMultiple()) {
               return true;
            }
         }
         return false;
      }

      @Override
      public ReflectionHelper.PropertyAccessor makeChildAttributeMetadata(ReflectionHelper.PropertyAccessor parentAttributeMetadata, String attribute) {
         return parentAttributeMetadata == null ?
               ReflectionHelper.getAccessor(clazz, attribute) : parentAttributeMetadata.getAccessor(attribute);
      }

      @Override
      public boolean isComparableProperty(ReflectionHelper.PropertyAccessor attributeMetadata) {
         Class<?> propertyType = attributeMetadata.getPropertyType();
         return propertyType != null && (propertyType.isPrimitive() || Comparable.class.isAssignableFrom(propertyType));
      }
   }
}
