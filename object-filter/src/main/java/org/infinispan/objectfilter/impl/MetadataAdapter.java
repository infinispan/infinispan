package org.infinispan.objectfilter.impl;

import java.util.List;

/**
 * A generic representation of some of the aspects of type metadata. This is meant to ensure decoupling from the
 * underlying metadata representation (Class, Protobuf descriptor, etc).
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface MetadataAdapter<TypeMetadata, AttributeMetadata, AttributeId extends Comparable<AttributeId>> {

   String getTypeName();

   TypeMetadata getTypeMetadata();

   /**
    * Transforms a String property path into an internal representation of the path which might not be String based
    * (AttributeId is an Integer in the Protobuf case).
    */
   List<AttributeId> translatePropertyPath(List<String> path);

   /**
    * Tests if the attribute path contains repeated (collection/array) attributes.
    */
   boolean isRepeatedProperty(List<String> propertyPath);

   AttributeMetadata makeChildAttributeMetadata(AttributeMetadata parentAttributeMetadata, AttributeId attribute);

   boolean isComparableProperty(AttributeMetadata attributeMetadata);
}
