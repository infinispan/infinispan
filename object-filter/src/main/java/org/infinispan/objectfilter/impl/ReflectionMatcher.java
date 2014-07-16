package org.infinispan.objectfilter.impl;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.FilterProcessingChain;
import org.infinispan.objectfilter.impl.hql.ReflectionEntityNamesResolver;
import org.infinispan.objectfilter.impl.hql.ReflectionPropertyHelper;
import org.infinispan.objectfilter.impl.predicateindex.MatcherEvalContext;
import org.infinispan.objectfilter.impl.predicateindex.ReflectionMatcherEvalContext;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionMatcher extends BaseMatcher<Class<?>, String> {

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
   protected MatcherEvalContext<String> startContext(Object instance, Set<String> knownTypes) {
      String typeName = instance.getClass().getCanonicalName();
      if (knownTypes.contains(typeName)) {
         return new ReflectionMatcherEvalContext(instance);
      } else {
         return null;
      }
   }

   @Override
   protected FilterProcessingChain<Class<?>> createFilterProcessingChain(Map<String, Object> namedParameters) {
      return FilterProcessingChain.build(entityNamesResolver, propertyHelper, namedParameters);
   }

   @Override
   protected FilterRegistry<String> createFilterRegistryForType(Class<?> clazz) {
      return new FilterRegistry<String>(new MetadataAdapterImpl(clazz));
   }

   private static class MetadataAdapterImpl implements MetadataAdapter<ReflectionHelper.PropertyAccessor, String> {

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
