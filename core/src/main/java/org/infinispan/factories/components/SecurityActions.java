package org.infinispan.factories.components;

import org.infinispan.commons.util.ReflectionUtil;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * SecurityActions for the org.infinispan.factories.components package.
 *
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Radoslav Husar
 * @since 10.0
 */
final class SecurityActions {

   static Method getMethod(Class<?> componentClass, ComponentMetadata.PrioritizedMethodMetadata prioritizedMethod) {
      Method method;
      if (System.getSecurityManager() == null) {
         method = ReflectionUtil.findMethod(componentClass, prioritizedMethod.getMethodName());
      } else {
         method = AccessController.doPrivileged((PrivilegedAction<Method>) () -> ReflectionUtil.findMethod(componentClass, prioritizedMethod.getMethodName()));
      }
      return method;
   }

   static Method getMethod(Class<?> componentClass, ComponentMetadata.InjectMethodMetadata methodMetadata, Class<?>[] parameterClasses) {
      Method method;
      if (System.getSecurityManager() == null) {
         method = ReflectionUtil.findMethod(componentClass, methodMetadata.getMethodName(), parameterClasses);
      } else {
         method = AccessController.doPrivileged((PrivilegedAction<Method>) () -> ReflectionUtil.findMethod(componentClass, methodMetadata.getMethodName(), parameterClasses));
      }
      return method;
   }

   static Field getField(ComponentMetadata.InjectFieldMetadata fieldMetadata, Class<?> declarationClass) {
      Field field;
      if (System.getSecurityManager() == null) {
         field = ReflectionUtil.getField(fieldMetadata.getFieldName(), declarationClass);
      } else {
         field = AccessController.doPrivileged((PrivilegedAction<Field>) () -> ReflectionUtil.getField(fieldMetadata.getFieldName(), declarationClass));
      }
      return field;
   }
}
