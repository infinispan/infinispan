package org.infinispan.container.versioning.irac;

import java.util.Collection;

import org.infinispan.commons.util.IntSet;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;

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
   public void storeTombstoneIfAbsent(IracTombstoneInfo tombstone) {
      //no-op
   }

   @Override
   public void removeTombstone(IracTombstoneInfo tombstone) {
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

   @Override
   public void sendStateTo(Address requestor, IntSet segments) {
      //no-op
   }

   @Override
   public void checkStaleTombstone(Collection<? extends IracTombstoneInfo> tombstones) {
      //no-op
   }
}
