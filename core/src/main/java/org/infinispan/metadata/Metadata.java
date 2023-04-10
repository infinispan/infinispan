package org.infinispan.metadata;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.Experimental;
import org.infinispan.container.versioning.EntryVersion;

/**
 * This interface encapsulates metadata information that can be stored
 * alongside values in the cache.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public interface Metadata {

   /**
    * Returns the lifespan of the cache entry with which this metadata object
    * is associated, in milliseconds.  Negative values are interpreted as
    * unlimited lifespan.
    *
    * @return lifespan of the entry in number of milliseconds
    */
   long lifespan();

   /**
    * Returns the the maximum amount of time that the cache entry associated
    * with this metadata object is allowed to be idle for before it is
    * considered as expired, in milliseconds.
    *
    * @return maximum idle time of the entry in number of milliseconds
    */
   long maxIdle();

   /**
    * Returns the version of the cache entry with which this metadata object
    * is associated.
    *
    * @return version of the entry
    */
   EntryVersion version();

   /**
    * Returns if the creation timestamp is updated when an entry is modified.
    * <p>
    * Created entries always update the creation timestamp.
    * <p>
    * This capability is experimental and all Infinispan implementations return {@code true}. To update creation
    * timestamps you must create a custom {@link Metadata} implementation.
    *
    * @return {@code true} to update the creation timestamp when entries are modified.
    */
   @Experimental
   default boolean updateCreationTimestamp() {
      return true;
   }

   /**
    * Returns whether this metadata is effectively empty, that is that persisting or replicating it to another
    * node would be no different then sending a null metadata object.
    * @return if this metadata has no actual data to store
    */
   default boolean isEmpty() {
      return false;
   }

   /**
    * Returns an instance of {@link Builder} which can be used to build
    * new instances of {@link Metadata} instance which are full copies of
    * this {@link Metadata}.
    *
    * @return instance of {@link Builder}
    */
   Builder builder();

   /**
    * Metadata builder
    */
   public interface Builder {

      /**
       * Set lifespan time with a given time unit.
       *
       * @param time of lifespan
       * @param unit unit of time for lifespan time
       * @return a builder instance with the lifespan time applied
       */
      Builder lifespan(long time, TimeUnit unit);

      /**
       * Set lifespan time assuming that the time unit is milliseconds.
       *
       * @param time of lifespan, in milliseconds
       * @return a builder instance with the lifespan time applied
       */
      Builder lifespan(long time);

      /**
       * Set max idle time with a given time unit.
       *
       * @param time of max idle
       * @param unit of max idle time
       * @return a builder instance with the max idle time applied
       */
      Builder maxIdle(long time, TimeUnit unit);

      /**
       * Set max idle time assuming that the time unit is milliseconds.
       *
       * @param time of max idle, in milliseconds
       * @return a builder instance with the max idle time applied
       */
      Builder maxIdle(long time);

      /**
       * Set version.
       *
       * @param version of the metadata
       * @return a builder instance with the version applied
       */
      Builder version(EntryVersion version);

      /**
       * Sets how the creation timestamp is updated.
       * <p>
       * Affects mortal entries only; in other words, for entries where {@link Metadata#lifespan()} is greater than
       * zero.
       * <p>
       * When {@code true} (default), the creation timestamp is updated when an entry is created or modified. When set
       * to {@code false}, the creation timestamp is updated only when the entry is created.
       * <p>
       * The capability is experimental and Infinispan {@link Metadata} and {@link Builder} does not implement this
       * method. To not update creation timestamps you must create a custom {@link Metadata} and {@link Builder}
       * implementation.
       *
       * @param enabled {@code false} to disable creation timestamp update when modifying entries.
       * @return a builder instance with the version applied.
       */
      @Experimental
      default Builder updateCreationTimestamp(boolean enabled) {
         //no-op by default
         return this;
      }

      /**
       * Build a metadata instance.
       *
       * @return an instance of metadata
       */
      Metadata build();

      /**
       * Merges the given metadata information into the given builder.
       *
       * @param metadata to merge into this builder
       * @return a builder instance with the metadata applied
       */
      Builder merge(Metadata metadata);
   }

}
