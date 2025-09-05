package org.infinispan.query.objectfilter.impl.predicateindex;

public interface MetadataProjectable<AttributeValue> {

   Object projection(Object key, Object instance, Object metadata, AttributeValue attribute);

}
