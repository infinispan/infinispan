package org.infinispan.configuration.parsing;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used in Infinispan
 *
 * @author Pete Muir
 */
public enum Attribute {
    // must be first
    UNKNOWN(null),

    // KEEP THESE IN ALPHABETICAL ORDER!

    ACQUIRE_TIMEOUT,
    ADDRESS_COUNT,
    AFTER,
    ALIASES,
    ALLOW_DUPLICATE_DOMAINS("duplicate-domains"),
    ASYNC_EXECUTOR("async-executor"),
    @Deprecated
    ASYNC_MARSHALLING,
    AUDIT_LOGGER,
    AUTO_COMMIT,
    AUTO_CONFIG,
    AVAILABILITY_INTERVAL,
    AWAIT_INITIAL_TRANSFER,
    BACKUP_FAILURE_POLICY("failure-policy"),
    BEFORE,
    CAPACITY_FACTOR("capacity"),
    CHUNK_SIZE,
    CLASS,
    CLUSTER,
    COMPLETED_TX_TIMEOUT("complete-timeout"),
    CONCURRENCY_LEVEL,
    CONFIGURATION,
    CONNECTION_ATTEMPTS,
    CONNECTION_INTERVAL,
    CONSISTENT_HASH_FACTORY,
    CORE_THREADS,
    DATA_CONTAINER,
    DEFAULT_CACHE,
    DEFAULT_STACK,
    ENABLED,
    ENCODER,
    EXECUTOR,
    EVICTION,
    @Deprecated
    EVICTION_EXECUTOR,
    EXPIRATION_EXECUTOR,
    EXTENDS,
    FAIL_SILENTLY,
    FAILURE_POLICY_CLASS,
    FETCH_STATE,
    @Deprecated
    FLUSH_LOCK_TIMEOUT,
    FRAGMENTATION_FACTOR,
    GROUP_NAME,
    ID,
    INDEX,
    INITIAL_CLUSTER_SIZE,
    INITIAL_CLUSTER_TIMEOUT,
    INTERVAL,
    INVALIDATION_BATCH_SIZE,
    INVALIDATION_CLEANUP_TASK_FREQUENCY("l1-cleanup-interval"),
    ISOLATION,
    JNDI_NAME,
    JMX_DOMAIN("domain"),
    KEEP_ALIVE_TIME("keepalive-time"),
    KEY,
    KEY_EQUIVALENCE,
    KEY_PARTITIONER,
    L1_LIFESPAN("l1-lifespan"),
    LIFESPAN,
    LISTENER_EXECUTOR,
    LOCATION,
    LOCK_TIMEOUT,
    LOCKING,
    MACHINE_ID("machine"),
    MAPPER,
    MARSHALLER_CLASS("marshaller"),
    MAX_BATCH_SIZE,
    MAX_ENTRIES,
    MAX_IDLE,
    MAX_RETRIES,
    MAX_THREADS,
    MBEAN_SERVER_LOOKUP,
    MERGE_POLICY,
    MEDIA_TYPE,
    MODE,
    NODE_NAME,
    MODIFICATION_QUEUE_SIZE,
    MODULE,
    NAME,
    NAMES,
    NOTIFICATIONS,
    ON_REHASH("onRehash"),
    OWNERS,
    PATH,
    PASSIVATION,
    PERMISSIONS,
    PERSISTENCE_EXECUTOR,
    POSITION,
    PRELOAD,
    PRIORITY,
    PURGE,
    @Deprecated
    QUEUE_FLUSH_INTERVAL,
    QUEUE_LENGTH,
    @Deprecated
    QUEUE_SIZE,
    RACK_ID("rack"),
    READ_ONLY,
    REAPER_WAKE_UP_INTERVAL("reaper-interval"),
    RECOVERY_INFO_CACHE_NAME("recovery-cache"),
    RELATIVE_TO,
    REMOTE_CACHE,
    REMOTE_COMMAND_EXECUTOR,
    REMOTE_SITE,
    REMOTE_TIMEOUT,
    @Deprecated
    REPLICATION_QUEUE_EXECUTOR,
    ROLES,
    SEGMENTED,
    SEGMENTS,
    SHARED,
    SHUTDOWN_HOOK,
    @Deprecated
    SHUTDOWN_TIMEOUT,
    SIMPLE_CACHE,
    @Deprecated
    SINGLETON,
    SITE,
    SIZE,
    @Deprecated
    SPIN_DURATION("deadlock-detection-spin"),
    STATISTICS,
    STATISTICS_AVAILABLE,
    START,
    STATE_TRANSFER_EXECUTOR,
    STORE_KEYS_AS_BINARY("keys"),
    STORE_VALUES_AS_BINARY("values"),
    STRATEGY,
    STRIPING,
    STACK,
    STOP_TIMEOUT,
    TAKE_BACKUP_OFFLINE_AFTER_FAILURES("after-failures"),
    TAKE_BACKUP_OFFLINE_MIN_WAIT("min-wait"),
    THREAD_FACTORY,
    THREAD_NAME_PATTERN,
    THREAD_POLICY,
    THREAD_POOL_SIZE,
    TIMEOUT,
    TOTAL_ORDER_EXECUTOR,
    TRANSACTION_MANAGER_LOOKUP_CLASS("transaction-manager-lookup"),
    TRANSACTION_PROTOCOL("protocol"),
    TRANSACTIONAL,
    TRANSFORMER,
    TRANSPORT,
    TYPE,
    UNRELIABLE_RETURN_VALUES,
    USE_TWO_PHASE_COMMIT("two-phase-commit"),
    VALUE,
    VALUE_EQUIVALENCE,
    VERSION,
    VERSIONING_SCHEME("scheme"),
    WAIT_TIME,
    WHEN_SPLIT,
    WRITE_SKEW_CHECK("write-skew"),
    ZERO_CAPACITY_NODE("zero-capacity-node")
    ;

    private final String name;

    Attribute(final String name) {
        this.name = name;
    }

    Attribute() {
        this.name = name().toLowerCase().replace('_', '-');
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Attribute> attributes;

    static {
        final Map<String, Attribute> map = new HashMap<>(64);
        for (Attribute attribute : values()) {
            final String name = attribute.getLocalName();
            if (name != null) map.put(name, attribute);
        }
        attributes = map;
    }

    public static Attribute forName(String localName) {
        final Attribute attribute = attributes.get(localName);
        return attribute == null ? UNKNOWN : attribute;
    }

   @Override
   public String toString() {
      return name;
   }
}
