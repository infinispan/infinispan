package org.infinispan.objectfilter.impl.predicateindex;

public interface MetadataProjectable<AttributeValue> {

   Object projection(Object key, AttributeValue attribute);

}
