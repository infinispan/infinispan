package org.infinispan.container.versioning.irac;

import org.infinispan.metadata.impl.IracMetadata;

/**
 * No-op implementation for {@link IracTombstoneManager}.
 * <p>
 * It is used when IRAC is not enabled.
 *
 * @since 14.0
 */
public final class NoOpIracTombstoneManager implements IracTombstoneManager {

   private static final NoOpIracTombstoneManager INSTANCE = new NoOpIracTombstoneManager();

   private NoOpIracTombstoneManager() {
   }

   public static NoOpIracTombstoneManager getInstance() {
      return INSTANCE;
   }

   @Override
   public void storeTombstone(int segment, Object key, IracMetadata metadata) {
      //no-op
   }

   @Override
   public void storeTombstoneIfAbsent(int segment, Object key, IracMetadata metadata) {
      //no-op
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      //no-op
   }

   @Override
   public void removeTombstone(Object key) {
      //no-op
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      return null;
   }

   @Override
   public boolean isEmpty() {
      return true;
   }

   @Override
   public int size() {
      return 0;
   }

   @Override
   public boolean isTaskRunning() {
      return false;
   }

   @Override
   public long getCurrentDelayMillis() {
      return 0;
   }
}
