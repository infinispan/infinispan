package org.infinispan.commons.dataconversion.internal;

/**
 * Interface for classes which can be serialized to JSON.
 *
 * @since 12.0
 */
public interface JsonSerialization {

   Json toJson();

}
