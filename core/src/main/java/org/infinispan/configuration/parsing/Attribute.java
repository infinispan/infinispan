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

    ACQUIRE_TIMEOUT("acquire-timeout"),
    AFTER("after"),
    ALIASES("aliases"),
    ALLOW_DUPLICATE_DOMAINS("duplicate-domains"),
    ASYNC_EXECUTOR("async-executor"),
    ASYNC_MARSHALLING("async-marshalling"),
    AUDIT_LOGGER("audit-logger"),
    AUTO_COMMIT("auto-commit"),
    AUTO_CONFIG("auto-config"),
    AWAIT_INITIAL_TRANSFER("await-initial-transfer"),
    BACKUP_FAILURE_POLICY("failure-policy"),
    BEFORE("before"),
    CAPACITY_FACTOR("capacity"),
    CHUNK_SIZE("chunk-size"),
    CLASS("class"),
    CLUSTER("cluster"),
    COMPLETED_TX_TIMEOUT("complete-timeout"),
    CONCURRENCY_LEVEL("concurrency-level"),
    CONFIGURATION("configuration"),
    CONSISTENT_HASH_FACTORY("consistent-hash-factory"),
    CORE_THREADS("core-threads"),
    DATA_CONTAINER("data-container"),
    DEFAULT_CACHE("default-cache"),
    ENABLED("enabled"),
    EXECUTOR("executor"),
    EXTENDS("extends"),
    @Deprecated
    EVICTION_EXECUTOR("eviction-executor"),
    EXPIRATION_EXECUTOR("expiration-executor"),
    FAILURE_POLICY_CLASS("failure-policy-class"),
    FETCH_STATE("fetch-state"),
    @Deprecated
    FLUSH_LOCK_TIMEOUT("flush-lock-timeout"),
    GROUP_NAME("group-name"),
    ID("id"),
    INDEX("index"),
    INLINE_INTERCEPTORS("inline-interceptors"),
    INTERVAL("interval"),
    INVALIDATION_CLEANUP_TASK_FREQUENCY("l1-cleanup-interval"),
    ISOLATION("isolation"),
    JNDI_NAME("jndi-name"),
    JMX_DOMAIN("domain"),
    KEEP_ALIVE_TIME("keepalive-time"),
    KEY_EQUIVALENCE("key-equivalence"),
    KEY_PARTITIONER("key-partitioner"),
    L1_LIFESPAN("l1-lifespan"),
    LIFESPAN("lifespan"),
    LISTENER_EXECUTOR("listener-executor"),
    LOCATION("location"),
    LOCK_TIMEOUT("lock-timeout"),
    LOCKING("locking"),
    MACHINE_ID("machine"),
    MAPPER("mapper"),
    MARSHALLER_CLASS("marshaller"),
    MAX_ENTRIES("max-entries"),
    MAX_IDLE("max-idle"),
    MAX_RETRIES("max-retries"),
    MAX_THREADS("max-threads"),
    MBEAN_SERVER_LOOKUP("mbean-server-lookup"),
    MODE("mode"),
    NODE_NAME("node-name"),
    MODIFICATION_QUEUE_SIZE("modification-queue-size"),
    MODULE("module"),
    NAME("name"),
    NAMES("names"),
    NOTIFICATIONS("notifications"),
    ON_REHASH("onRehash"),
    OWNERS("owners"),
    PATH("path"),
    PASSIVATION("passivation"),
    PERMISSIONS("permissions"),
    PERSISTENCE_EXECUTOR("persistence-executor"),
    POSITION("position"),
    PRELOAD("preload"),
    PRIORITY("priority"),
    PURGE("purge"),
    QUEUE_FLUSH_INTERVAL("queue-flush-interval"),
    QUEUE_LENGTH("queue-length"),
    QUEUE_SIZE("queue-size"),
    RACK_ID("rack"),
    READ_ONLY("read-only"),
    REAPER_WAKE_UP_INTERVAL("reaper-interval"),
    RECOVERY_INFO_CACHE_NAME("recovery-cache"),
    RELATIVE_TO("relative-to"),
    REMOTE_CACHE("remote-cache"),
    REMOTE_COMMAND_EXECUTOR("remote-command-executor"),
    REMOTE_SITE("remote-site"),
    REMOTE_TIMEOUT("remote-timeout"),
    REPLICATION_QUEUE_EXECUTOR("replication-queue-executor"),
    ROLES("roles"),
    SEGMENTS("segments"),
    SHARED("shared"),
    SHUTDOWN_HOOK("shutdown-hook"),
    @Deprecated
    SHUTDOWN_TIMEOUT("shutdown-timeout"),
    SIMPLE_CACHE("simple-cache"),
    SINGLETON("singleton"),
    SITE("site"),
    SIZE("size"),
    SPIN_DURATION("deadlock-detection-spin"),
    STATISTICS("statistics"),
    STATISTICS_AVAILABLE("statistics-available"),
    START("start"),
    STATE_TRANSFER_EXECUTOR("state-transfer-executor"),
    STORE_KEYS_AS_BINARY("keys"),
    STORE_VALUES_AS_BINARY("values"),
    STRATEGY("strategy"),
    STRIPING("striping"),
    STACK("stack"),
    STOP_TIMEOUT("stop-timeout"),
    TAKE_BACKUP_OFFLINE_AFTER_FAILURES("after-failures"),
    TAKE_BACKUP_OFFLINE_MIN_WAIT("min-wait"),
    THREAD_FACTORY("thread-factory"),
    THREAD_NAME_PATTERN("thread-name-pattern"),
    THREAD_POLICY("thread-policy"),
    THREAD_POOL_SIZE("thread-pool-size"),
    TIMEOUT("timeout"),
    TOTAL_ORDER_EXECUTOR("total-order-executor"),
    TRANSACTION_MANAGER_LOOKUP_CLASS("transaction-manager-lookup"),
    TRANSACTION_PROTOCOL("protocol"),
    TRANSPORT("transport"),
    TYPE("type"),
    UNRELIABLE_RETURN_VALUES("unreliable-return-values"),
    USE_TWO_PHASE_COMMIT("two-phase-commit"),
    VALUE("value"),
    VALUE_EQUIVALENCE("value-equivalence"),
    VERSION("version"),
    VERSIONING_SCHEME("scheme"),
    WAIT_TIME("wait-time"),
    WRITE_SKEW_CHECK("write-skew"),
    FRAGMENTATION_FACTOR("fragmentation-factor"),
    ;

    private final String name;

    private Attribute(final String name) {
        this.name = name;
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
        final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
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
}
