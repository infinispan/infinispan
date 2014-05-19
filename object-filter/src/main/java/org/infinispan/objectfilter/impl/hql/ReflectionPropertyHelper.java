package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;
import org.jboss.logging.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class ReflectionPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   private static final Log log = Logger.getMessageLogger(Log.class, ReflectionPropertyHelper.class.getName());

   private static final Set<Class<?>> primitives = new HashSet<Class<?>>();

   static {
      //todo [anistor] handle arrays and collections
      primitives.add(java.util.Date.class);
      primitives.add(String.class);
      primitives.add(Character.class);
      primitives.add(char.class);
      primitives.add(Double.class);
      primitives.add(double.class);
      primitives.add(Float.class);
      primitives.add(float.class);
      primitives.add(Long.class);
      primitives.add(long.class);
      primitives.add(Integer.class);
      primitives.add(int.class);
      primitives.add(Short.class);
      primitives.add(short.class);
      primitives.add(Byte.class);
      primitives.add(byte.class);
      primitives.add(Boolean.class);
      primitives.add(boolean.class);
   }

   private final ClassLoader classLoader;

   private final EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
      @Override
      public Class<?> getClassFromName(String entityName) {
         if (classLoader != null) {
            try {
               return classLoader.loadClass(entityName);
            } catch (ClassNotFoundException e) {
               return null;
            }
         }

         try {
            return Class.forName(entityName);
         } catch (ClassNotFoundException e) {
            return null;
         }
      }
   };

   public ReflectionPropertyHelper(ClassLoader classLoader) {
      this.classLoader = classLoader;
   }

   @Override
   public EntityNamesResolver getEntityNamesResolver() {
      return entityNamesResolver;
   }

   @Override
   public Class<?> getEntityMetadata(String targetTypeName) {
      return entityNamesResolver.getClassFromName(targetTypeName);
   }

   @Override
   public Class<?> getPropertyType(String entityType, List<String> propertyPath) {
      Class<?> type = entityNamesResolver.getClassFromName(entityType);
      if (type == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      return getPropertyTypeByPath(type, propertyPath);
   }

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
         Class<?> propType = getPropertyTypeByPath(entity, propertyPath);
         return propType != null && !primitives.contains(propType);
      } catch (Exception e) {
         return false; // todo [anistor] need clean solution
      }
   }

   private boolean hasProperty(String entityType, String... propertyPath) {
      Class<?> entity = entityNamesResolver.getClassFromName(entityType);
      if (entity == null) {
         throw new IllegalStateException("Unknown entity name " + entityType);
      }

      try {
         Class<?> propType = getPropertyTypeByPath(entity, Arrays.asList(propertyPath));
         return propType != null;
      } catch (Exception e) {
         return false; // todo [anistor] need clean solution
      }
   }

   private Class<?> getPropertyTypeByPath(Class<?> entityClass, List<String> propertyPath) {
      ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(entityClass, propertyPath.get(0));
      for (int i = 1; i < propertyPath.size(); i++) {
         accessor = accessor.getAccessor(propertyPath.get(i));
      }
      return accessor.getPropertyType();
   }
}
