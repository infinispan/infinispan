package org.infinispan.objectfilter.impl.syntax.parser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface EntityNameResolver {

   //todo [anistor] now that all types are predeclared we could be able to resolve unqualified type names too
   Class<?> resolve(String name);
}
