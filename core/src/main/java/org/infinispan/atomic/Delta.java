package org.infinispan.atomic;

/**
 * Represents changes made to a {@link DeltaAware} implementation.  Implementations should be efficiently
 * {@link java.io.Externalizable} rather than just {@link java.io.Serializable}.
 *
 * @see DeltaAware
 * @author Manik Surtani
 * @since 4.0
 * @deprecated since 9.1
 */
@Deprecated
public interface Delta {
   /**
    * Merge the current Delta instance with a given {@link DeltaAware} instance, and return a coherent and complete
    * {@link DeltaAware} instance.  Implementations should be able to deal with null values passed in, or values of a
    * different type from the expected DeltaAware instance.  Usually the approach would be to ignore what is passed in,
    * create a new instance of the DeltaAware implementation that the current Delta implementation is written for, apply
    * changes and pass it back.
    *
    * @param d instance to merge with, or null if no merging is needed
    * @return a fully coherent and usable instance of DeltaAware which may or may not be the same instance passed in
    */
   DeltaAware merge(DeltaAware d);
}
