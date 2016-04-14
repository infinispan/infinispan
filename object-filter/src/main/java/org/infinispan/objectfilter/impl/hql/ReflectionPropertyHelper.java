package org.infinispan.objectfilter.impl.hql;

import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.infinispan.objectfilter.impl.logging.Log;
import org.infinispan.objectfilter.impl.util.ReflectionHelper;
import org.jboss.logging.Logger;

import java.beans.IntrospectionException;
import java.util.Arrays;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class ReflectionPropertyHelper extends ObjectPropertyHelper<Class<?>> {

   private static final Log log = Logger.getMessageLogger(Log.class, ReflectionPropertyHelper.class.getName());

   public ReflectionPropertyHelper(EntityNamesResolver entityNamesResolver) {
      super(entityNamesResolver);
   }

   @Override
   public Class<?> getEntityMetadata(String targetTypeName) {
      return entityNamesResolver.getClassFromName(targetTypeName);
   }

   @Override
   public List<?> mapPropertyNamePathToFieldIdPath(Class<?> type, String[] propertyPath) {
      return Arrays.asList(propertyPath);
   }

   @Override
   public Class<?> getPrimitivePropertyType(String entityType, String[] propertyPath) {
      Class<?> entityClass = getEntityClass(entityType);

      try {
         Class<?> propType = getPropertyAccessor(entityClass, propertyPath).getPropertyType();
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
   public boolean hasEmbeddedProperty(String entityType, String[] propertyPath) {
      Class<?> entityClass = getEntityClass(entityType);

      try {
         Class<?> propType = getPropertyAccessor(entityClass, propertyPath).getPropertyType();
         return propType != null && !propType.isEnum() && !primitives.containsKey(propType);
      } catch (IntrospectionException e) {
         return false;
      }
   }

   @Override
   public boolean isRepeatedProperty(String entityType, String[] propertyPath) {
      Class<?> entityClass = getEntityClass(entityType);

      try {
         ReflectionHelper.PropertyAccessor a = ReflectionHelper.getAccessor(entityClass, propertyPath[0]);
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
      Class<?> entityClass = getEntityClass(entityType);

      try {
         Class<?> propType = getPropertyAccessor(entityClass, propertyPath).getPropertyType();
         return propType != null;
      } catch (IntrospectionException e) {
         return false;
      }
   }

   private Class<?> getEntityClass(String entityType) {
      Class<?> entityClass = getEntityMetadata(entityType);
      if (entityClass == null) {
         throw log.getUnknownEntity(entityType);
      }
      return entityClass;
   }

   private ReflectionHelper.PropertyAccessor getPropertyAccessor(Class<?> entityClass, String[] propertyPath) throws IntrospectionException {
      ReflectionHelper.PropertyAccessor accessor = ReflectionHelper.getAccessor(entityClass, propertyPath[0]);
      for (int i = 1; i < propertyPath.length; i++) {
         accessor = accessor.getAccessor(propertyPath[i]);
      }
      return accessor;
   }
}
