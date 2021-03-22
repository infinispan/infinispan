package org.infinispan.api.mutiny;

/**
 * A {@link MutinyCache} entry status. Used by listeners and Continuous Query
 *
 * @since 14.0
 */
public enum EntryStatus {
   CREATED, UPDATED, DELETED
}
