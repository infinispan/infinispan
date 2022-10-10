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
    @Deprecated
    ALLOW_DUPLICATE_DOMAINS("duplicate-domains"),
    @Deprecated
    ASYNC_EXECUTOR,
    @Deprecated
    ASYNC_MARSHALLING,
    AUDIT_LOGGER,
    AUTO_COMMIT,
    @Deprecated
    AUTO_CONFIG,
    AVAILABILITY_INTERVAL,
    AWAIT_INITIAL_TRANSFER,
    BACKUP_FAILURE_POLICY("failure-policy"),
    BEFORE,
    BIAS_ACQUISITION,
    BIAS_LIFESPAN,
    BLOCKING_EXECUTOR,
    CACHE_SIZE,
    CACHE_TIMEOUT,
    CALIBRATE_BY_DELETES,
    @Deprecated
    CAPACITY,
    CAPACITY_FACTOR,
    CHUNK_SIZE,
    CLASS,
    CLUSTER,
    COMMIT_INTERVAL,
    COMPACTION_THRESHOLD,
    COMPLETED_TX_TIMEOUT("complete-timeout"),
    CONCURRENCY_LEVEL,
    CONFIGURATION,
    CONNECTION_ATTEMPTS,
    CONNECTION_INTERVAL,
    CONSISTENT_HASH_FACTORY,
    CONTEXT_INITIALIZER,
    CORE_THREADS,
    DATA_CONTAINER,
    DEFAULT_CACHE,
    DEFAULT_STACK,
    DEFAULT_MAX_RESULTS,
    DOMAIN,
    ENABLED,
    ENCODER,
    EXECUTOR,
    EVICTION,
    @Deprecated
    EVICTION_EXECUTOR,
    @Deprecated
    EVICTION_STRATEGY,
    EXPIRATION_EXECUTOR,
    EXTENDS,
    FACTOR,
    FAIL_SILENTLY,
    FAILURE_POLICY_CLASS,
    FETCH_STATE,
    @Deprecated
    FLUSH_LOCK_TIMEOUT,
    FRAGMENTATION_FACTOR,
    GAUGES,
    GROUP_NAME,
    HISTOGRAMS,
    ID,
    @Deprecated
    INDEX,
    INDEX_QUEUE_LENGTH("max-queue-length"),
    INDEXED_ENTITIES,
    INITIAL_CLUSTER_SIZE,
    INITIAL_CLUSTER_TIMEOUT,
    INTERVAL,
    INVALIDATION_BATCH_SIZE,
    INVALIDATION_CLEANUP_TASK_FREQUENCY("l1-cleanup-interval"),
    ISOLATION,
    JNDI_NAME,
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
    LOW_LEVEL_TRACE,
    MACHINE_ID("machine"),
    MAPPER,
    MARSHALLER,
    MAX_BATCH_SIZE,
    MAX_CLEANUP_DELAY,
    MAX_COUNT,
    MAX_ENTRIES,
    MAX_FILE_SIZE,
    MAX_IDLE,
    MAX_NODE_SIZE,
    MAX_RETRIES,
    MIN_SIZE,
    MAX_BUFFERED_ENTRIES,
    MAX_FORCED_SIZE,
    MAX_SIZE,
    MAX_THREADS,
    MBEAN_SERVER_LOOKUP,
    MERGE_POLICY,
    MEDIA_TYPE,
    MIN_NODE_SIZE,
    MODE,
    NODE_NAME,
    MODIFICATION_QUEUE_SIZE,
    MODULE,
    NAME,
    NAMES,
    NAMES_AS_TAGS("namesAsTags"),
    NON_BLOCKING_EXECUTOR,
    NOTIFICATIONS,
    ON_REHASH("onRehash"),
    OPEN_FILES_LIMIT,
    OWNERS,
    PATH,
    PASSIVATION,
    PERMISSIONS,
    @Deprecated
    PERSISTENCE_EXECUTOR,
    POSITION,
    PREFIX,
    PRELOAD,
    PRIORITY,
    PROPERTIES,
    PURGE,
    QUEUE_COUNT,
    @Deprecated
    QUEUE_FLUSH_INTERVAL,
    QUEUE_LENGTH,
    QUEUE_SIZE,
    RACK_ID("rack"),
    RAM_BUFFER_SIZE,
    RAFT_MEMBERS,
    READ_ONLY,
    REAPER_WAKE_UP_INTERVAL("reaper-interval"),
    RECOVERY_INFO_CACHE_NAME("recovery-cache"),
    REFRESH_INTERVAL,
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
    STARTUP_MODE,
    @Deprecated
    STATE_TRANSFER_EXECUTOR,
    STORAGE,
    STORE_KEYS_AS_BINARY("keys"),
    STORE_VALUES_AS_BINARY("values"),
    STRATEGY,
    STRIPING,
    STACK,
    STOP_TIMEOUT,
    SYNC_WRITES,
    TAKE_BACKUP_OFFLINE_AFTER_FAILURES("after-failures"),
    TAKE_BACKUP_OFFLINE_MIN_WAIT("min-wait"),
    THREAD_FACTORY,
    THREAD_NAME_PATTERN,
    THREAD_POLICY,
    THREAD_POOL_SIZE,
    TIMEOUT,
    TOMBSTONE_MAP_SIZE,
    TOTAL_ORDER_EXECUTOR,
    TOUCH,
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
    WHEN_FULL,
    WRITE_ONLY,
    WRITE_SKEW_CHECK("write-skew"),
    ZERO_CAPACITY_NODE,
    INVALIDATION_THRESHOLD,
    CONTEXT_INITIALIZERS,
    GROUPER,
    ACCURATE_SIZE,
    REGEX;

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
