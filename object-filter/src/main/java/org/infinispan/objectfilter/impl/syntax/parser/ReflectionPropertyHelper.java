package org.infinispan.objectfilter.impl.syntax.parser;

import java.beans.IntrospectionException;
import java.util.Arrays;
import java.util.List;

import org.infinispan.objectfilter.impl.util.ReflectionHelper;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   private final EntityNameResolver entityNameResolver;

   public ReflectionPropertyHelper(EntityNameResolver entityNameResolver) {
      if (entityNameResolver == null) {
         throw new IllegalArgumentException("The entityNameResolver argument cannot be null");
      }
      this.entityNameResolver = entityNameResolver;
   }

   @Override
   public Class<?> getEntityMetadata(String typeName) {
      return entityNameResolver.resolve(typeName);
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(Class<?> type, String[] propertyPath) {
      return Arrays.asList(propertyPath);
   }

   @Override
   public Class<?> getPrimitivePropertyType(Class<?> entityType, String[] propertyPath) {
      try {
         Class<?> propType = getPropertyAccessor(entityType, propertyPath).getPropertyType();
         if (propType.isEnum()) {
            return propType;
         }
         return primitives.get(propType);
      } catch (IntrospectionException e) {
         // ignored
      }
      return null;
   }

   @Override
   public boolean hasEmbeddedProperty(Class<?> entityType, String[] propertyPath) {
      try {
         Class<?> propType = getPropertyAccessor(entityType, propertyPath).getPropertyType();
         return propType != null && !propType.isEnum() && !primitives.containsKey(propType);
      } catch (IntrospectionException e) {
         return false;
      }
   }

   @Override
   public boolean isRepeatedProperty(Class<?> entityType, String[] propertyPath) {
      try {
         ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(entityType, propertyPath[0]);
         if (a.isMultiple()) {
            return true;
         }
         for (int i = 1; i < propertyPath.length; i++) {
            a = a.getAccessor(propertyPath[i]);
            if (a.isMultiple()) {
               return true;
            }
         }
      } catch (IntrospectionException e) {
         // ignored
      }
      return false;
   }

   @Override
   public boolean hasProperty(Class<?> entityType, String[] propertyPath) {
      try {
         Class<?> propType = getPropertyAccessor(entityType, propertyPath).getPropertyType();
         return propType != null;
      } catch (IntrospectionException e) {
         return false;
      }
   }

   private ReflectionHelper.PropertyAccessor getPropertyAccessor(Class<?> entityClass, String[] propertyPath) throws IntrospectionException {
      if (propertyPath == null || propertyPath.length == 0) {
         throw new IllegalArgumentException("propertyPath name cannot be null or empty");
      }
      ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(entityClass, propertyPath[0]);
      for (int i = 1; i < propertyPath.length; i++) {
         accessor = accessor.getAccessor(propertyPath[i]);
      }
      return accessor;
   }
}
