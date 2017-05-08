package org.infinispan.test;

import java.lang.reflect.InvocationTargetException;

import org.mockito.invocation.InvocationOnMock;

/**
 * Utility methods for dealing with Mockito mocks.
 *
 * @author Dan Berindei
 * @since 9.0
 */
public class Mocks {
   /**
    * Delegates a Mockito invocation to a target object, and returns the mock instead of the target object.
    *
    * Useful when {@code Mockito.spy(object)} doesn't work and the mocked class has a fluent interface.
    */
   public static <T> Object invokeAndReturnMock(InvocationOnMock i, T target)
         throws IllegalAccessException, InvocationTargetException {
      Object returnValue = i.getMethod().invoke(target, i.getArguments());
      // If necessary, replace the return value with the mock
      return (returnValue == target) ? i.getMock() : returnValue;
   }
}
