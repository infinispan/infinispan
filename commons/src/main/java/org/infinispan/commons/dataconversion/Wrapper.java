package org.infinispan.commons.dataconversion;

/**
 * A Wrapper is used to decorate objects produced by the {@link Encoder}.
 * A Wrapper, contrary to the Encoder, does not cause data conversion and it's used to provide additional
 * behaviour to the encoded data such as equality/hashCode and indexing capabilities.
 *
 * @since 9.1
 */
public interface Wrapper {

   Object wrap(Object obj);

   Object unwrap(Object obj);

   byte id();

   /**
    * @return true if the wrapped format is suitable to be indexed or filtered, thus avoiding extra unwrapping.
    */
   boolean isFilterable();

}
