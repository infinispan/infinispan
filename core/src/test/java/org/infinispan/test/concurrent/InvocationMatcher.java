package org.infinispan.test.concurrent;

/**
 * Matches method invocations by name, parameters and/or target.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public interface InvocationMatcher {
   boolean accept(Object instance, String methodName, Object[] arguments);
}
