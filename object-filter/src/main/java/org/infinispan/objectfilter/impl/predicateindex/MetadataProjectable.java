package org.infinispan.objectfilter.impl.predicateindex;

public interface MetadataProjectable {

   Object projection(Object key, String attribute);

}
