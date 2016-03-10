package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;

import java.beans.IntrospectionException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   private static final Map<Class<?>, Class<?>> primitives = new HashMap<>();

   static {
      primitives.put(java.util.Date.class, java.util.Date.class);
      primitives.put(java.time.Instant.class, java.time.Instant.class);
      primitives.put(String.class, String.class);
      primitives.put(Character.class, Character.class);
      primitives.put(char.class, Character.class);
      primitives.put(Double.class, Double.class);
      primitives.put(double.class, Double.class);
      primitives.put(Float.class, Float.class);
      primitives.put(float.class, Float.class);
      primitives.put(Long.class, Long.class);
      primitives.put(long.class, Long.class);
      primitives.put(Integer.class, Integer.class);
      primitives.put(int.class, Integer.class);
      primitives.put(Short.class, Short.class);
      primitives.put(short.class, Short.class);
      primitives.put(Byte.class, Byte.class);
      primitives.put(byte.class, Byte.class);
      primitives.put(Boolean.class, Boolean.class);
      primitives.put(boolean.class, Boolean.class);
   }

   public ReflectionPropertyHelper(EntityNamesResolver entityNamesResolver) {
      super(entityNamesResolver);
   }

   @Override
   public Class<?> getEntityMetadata(String targetTypeName) {
      return entityNamesResolver.getClassFromName(targetTypeName);
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, List<String> propertyPath) {
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
   public boolean hasProperty(String entityType, List<String> propertyPath) {
      return hasProperty(entityType, propertyPath.toArray(new String[propertyPath.size()]));
   }

   @Override
   public boolean hasEmbeddedProperty(String entityType, List<String> propertyPath) {
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
   public boolean isRepeatedProperty(String entityType, List<String> propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }
      try {
         ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(entity, propertyPath.get(0));
         if (a.isMultiple()) {
            return true;
         }
         for (int i = 1; i < propertyPath.size(); i++) {
            a = a.getAccessor(propertyPath.get(i));
            if (a.isMultiple()) {
               return true;
            }
         }
      } catch (IntrospectionException e) {
         // ignored
      }
      return false;
   }

   private boolean hasProperty(String entityType, String... propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      try {
         Class<?> propType = getPropertyAccessor(entity, Arrays.asList(propertyPath)).getPropertyType();
         return propType != null;
      } catch (IntrospectionException e) {
         return false;
      }
   }

   private ReflectionHelper.PropertyAccessor getPropertyAccessor(Class<?> entityClass, List<String> propertyPath) throws IntrospectionException {
      ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(entityClass, propertyPath.get(0));
      for (int i = 1; i < propertyPath.size(); i++) {
         accessor = accessor.getAccessor(propertyPath.get(i));
      }
      return accessor;
   }
}
