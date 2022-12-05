package org.infinispan.marshall.core;

import java.lang.reflect.Method;

/**
 * SecurityActions for the {@link org.infinispan.marshall.core} package.
 * <p>
 * Do not move. Do not change class and method visibility to avoid being called from other
 * {@link java.security.CodeSource}s, thus granting privilege escalation to external code.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
final class SecurityActions {

   static Method getMethodAndSetAccessible(Object o, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
      return getMethodAndSetAccessible(o.getClass(), methodName, parameterTypes);
   }

   static Method getMethodAndSetAccessible(Class<?> clazz, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
      Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
      method.setAccessible(true);
      return method;
   }
}
