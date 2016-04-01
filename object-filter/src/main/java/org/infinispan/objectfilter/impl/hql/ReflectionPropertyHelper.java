package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.beans.IntrospectionException;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   public ReflectionPropertyHelper(EntityNamesResolver entityNamesResolver) {
      super(entityNamesResolver);
   }

   @Override
   public Class<?> getEntityMetadata(String targetTypeName) {
      return entityNamesResolver.getClassFromName(targetTypeName);
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, String[] propertyPath) {
      Class<?> type = entityNamesResolver.getClassFromName(entityType);
      if (type == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      try {
         Class<?> propType = getPropertyAccessor(type, propertyPath).getPropertyType();
         if (propType.isEnum()) {
            return propType;
         }
         if (primitives.containsKey(propType)) {
            return primitives.get(propType);
         }
      } catch (IntrospectionException e) {
         // ignored
      }
      return null;
   }

   @Override
   public boolean hasEmbeddedProperty(String entityType, String[] propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      try {
         Class<?> propType = getPropertyAccessor(entity, propertyPath).getPropertyType();
         return propType != null && !propType.isEnum() && !primitives.containsKey(propType);
      } catch (IntrospectionException e) {
         return false;
      }
   }

   @Override
   public boolean isRepeatedProperty(String entityType, String[] propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }
      try {
         ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(entity, propertyPath[0]);
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
   public boolean hasProperty(String entityType, String[] propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      try {
         Class<?> propType = getPropertyAccessor(entity, propertyPath).getPropertyType();
         return propType != null;
      } catch (IntrospectionException e) {
         return false;
      }
   }

   private ReflectionHelper.PropertyAccessor getPropertyAccessor(Class<?> entityClass, String[] propertyPath) throws IntrospectionException {
      ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(entityClass, propertyPath[0]);
      for (int i = 1; i < propertyPath.length; i++) {
         accessor = accessor.getAccessor(propertyPath[i]);
      }
      return accessor;
   }
}
