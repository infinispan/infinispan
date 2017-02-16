package org.infinispan.objectfilter.impl.syntax.parser;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface TypeDescriptor<TypeMetadata> {

   /**
    * Returns the type name of the represented entity.
    *
    * @return the type name of the represented entity.
    */
   String getTypeName();

   /**
    * Returns the actual internal representation the entity. It might be a Class, or anything else.
    *
    * @return the internal representation the entity.
    */
   TypeMetadata getTypeMetadata();

   String[] makePath(String propName);
}
