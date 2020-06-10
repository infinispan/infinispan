package org.infinispan.objectfilter.impl.syntax.parser;

/**
 * @author anistor@redhat.com
 * @since 9.0
 */
@FunctionalInterface
public interface EntityNameResolver<TypeMetadata> {

   //TODO [anistor] ISPN-11986, now that all types are predeclared for indexed caches we could be able to resolve unqualified type names too
   TypeMetadata resolve(String typeName);
}
