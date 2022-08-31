package org.infinispan.persistence.manager;

/**
 * Identifies {@link PreloadManager}'s current status.
 *
 * @since 14.0
 */
public enum PreloadStatus {

   /**
    * The initial status when instantiated.
    * This status represents a {@link PreloadManager} that never loaded any data. Once the manager loads data, this
    * status is never reached again.
    */
   NOT_RUNNING,

   /**
    * Represents a {@link PreloadManager} that is currently loading data. The manager moves to this status before
    * loading data and proceeds to the next status after removing older entries.
    */

   RUNNING,

   /**
    * Represents a {@link PreloadManager} that finished loading and removing older entries successfully.
    */
   COMPLETE_LOAD,

   /**
    * Represents a {@link PreloadManager} that was unable to load the whole data. This happens when a cache configured
    * with a maximum number of entries is not filled. For example, the underlying storage has fewer entries.
    */
   PARTIAL_LOAD,

   /**
    * Represents a {@link PreloadManager} that failed either during load or during deletion of older entries.
    */
   FAILED_LOAD;

   public boolean fullyPreloaded() {
      return this == COMPLETE_LOAD;
   }
}
