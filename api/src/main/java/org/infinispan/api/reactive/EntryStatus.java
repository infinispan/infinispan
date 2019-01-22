package org.infinispan.api.reactive;

/**
 * A {@link KeyValueStore} entry status. Used by listeners and Continuous Query
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 10.0
 */
public enum EntryStatus {
   CREATED, UPDATED, DELETED
}
