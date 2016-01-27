package org.infinispan.distribution.ch.impl;

/**
 * Constants used as keys within a persisted consistent hash
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
public class ConsistentHashPersistenceConstants {
    public static final String STATE_CONSISTENT_HASH = "consistentHash";
    public static final String STATE_HASH_FUNCTION = "hashFunction";
    public static final String STATE_MEMBER = "member.%d";
    public static final String STATE_MEMBERS = "members";
}
