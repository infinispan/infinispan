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
    AFTER_FAILURES("afterFailures"),
    ALIASES("aliases"),
    ALLOW_DUPLICATE_DOMAINS("duplicate-domains"),
    ALWAYS_PROVIDE_IN_MEMORY_STATE("alwaysProvideInMemoryState"),
    ASYNC_MARSHALLING("async-marshalling"),
    AUTO_COMMIT("auto-commit"),
    AWAIT_INITIAL_TRANSFER("awaitInitialTransfer"),
    BACKUP_FAILURE_POLICY("failure-policy"),
    BACKUP_SITES("backupSites"),
    BATCHING("batching"),
    BEFORE("before"),
    CACHE_MANAGER_NAME("cacheManagerName"),
    CACHE_STOP_TIMEOUT("cacheStopTimeout"),
    CAPACITY_FACTOR("capacity"),
    CHUNK_SIZE("chunk-size"),
    CLASS("class"),
    CLUSTER_NAME("clusterName"),
    CLUSTER("cluster"),
    COMPLETED_TX_TIMEOUT("complete-timeout"),
    CONCURRENCY_LEVEL("concurrency-level"),
    CORE_THREADS("core-threads"),
    DATA_CONTAINER("data-container"),
    DEFAULT_CACHE("default-cache"),
    DEFENSIVE("defensive"),
    DISTRIBUTED_SYNC_TIMEOUT("distributedSyncTimeout"),
    EAGER_LOCK_SINGLE_NODE("eagerLockSingleNode"),
    ENABLED("enabled"),
    EXECUTOR("executor"),
    EXTERNALIZER_CLASS("externalizerClass"),
    EVICTION_EXECUTOR("eviction-executor"),
    FACTORY("factory"),
    FAILURE_POLICY_CLASS("failure-policy-class"),
    FETCH_IN_MEMORY_STATE("fetchInMemoryState"),
    FETCH_PERSISTENT_STATE("fetchPersistentState"),
    FETCH_STATE("fetch-state"),
    FLUSH_LOCK_TIMEOUT("flush-lock-timeout"),
    FSYNC_INTERVAL("fsyncInterval"),
    FSYNC_MODE("fsyncMode"),
    GROUP_NAME("group-name"),
    HASH_FUNCTION_CLASS("hashFunctionClass"),
    HASH_SEED_CLASS("hashSeedClass"),
    HOOK_BEHAVIOR("hookBehavior"),
    ID("id"),
    IGNORE_MODIFICATIONS("ignoreModifications"),
    INDEX("index"),
    INDEX_LOCAL_ONLY("indexLocalOnly"),
    INITIAL_RETRY_WAIT_TIME("initialRetryWaitTime"),
    INTERVAL("interval"),
    INVALIDATION_CLEANUP_TASK_FREQUENCY("l1-cleanup-interval"),
    INVALIDATION_THRESHOLD("invalidationThreshold"),
    ISOLATION_LEVEL("isolationLevel"),
    ISOLATION("isolation"),
    JNDI_NAME("jndi-name"),
    JMX_DOMAIN("domain"),
    KEEP_ALIVE_TIME("keepalive-time"),
    KEY_EQUIVALENCE("key-equivalence"),
    L1_LIFESPAN("l1-lifespan"),
    LIFESPAN("lifespan"),
    LISTENER_EXECUTOR("listener-executor"),
    LOCATION("location"),
    LOCK_ACQUISITION_TIMEOUT("lockAcquisitionTimeout"),
    LOCK_TIMEOUT("lock-timeout"),
    LOCKING("locking"),
    LOCKING_MODE("lockingMode"),
    LOG_FLUSH_TIMEOUT("logFlushTimeout"),
    MACHINE_ID("machine"),
    MAPPER("mapper"),
    MARSHALLER_CLASS("marshaller"),
    MAX_ENTRIES("max-entries"),
    MAX_IDLE("max-idle"),
    MAX_NON_PROGRESSING_LOG_WRITES("maxProgressingLogWrites"),
    MAX_THREADS("max-threads"),
    MBEAN_SERVER_LOOKUP("mbean-server-lookup"),
    MIN_TIME_TO_WAIT("minTimeToWait"),
    MODE("mode"),
    NODE_NAME("node-name"),
    MODIFICATION_QUEUE_SIZE("modification-queue-size"),
    MODULE("module"),
    NAME("name"),
    NUM_OWNERS("numOwners"),
    NUM_SEGMENTS("numSegments"),
    NUM_RETRIES("numRetries"),
    NUM_VIRTUAL_NODES("numVirtualNodes"),
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
    PURGE_ON_STARTUP("purgeOnStartup"),
    PURGE_SYNCHRONOUSLY("purgeSynchronously"),
    PURGER_THREADS("purgerThreads"),
    PUSH_STATE_TIMEOUT("pushStateTimeout"),
    PUSH_STATE_WHEN_COORDINATOR("pushStateWhenCoordinator"),
    QUEUE_FLUSH_INTERVAL("queue-flush-interval"),
    QUEUE_LENGTH("queue-length"),
    QUEUE_SIZE("queue-size"),
    RACK_ID("rack"),
    READ_ONLY("read-only"),
    REAPER_ENABLED("reaperEnabled"),
    REAPER_WAKE_UP_INTERVAL("reaper-interval"),
    RECOVERY_INFO_CACHE_NAME("recovery-cache"),
    REHASH_ENABLED("rehashEnabled"),
    REHASH_RPC_TIMEOUT("rehashRpcTimeout"),
    REHASH_WAIT("rehashWait"),
    RELATIVE_TO("relative-to"),
    REMOTE_CACHE("remote-cache"),
    REMOTE_CALL_TIMEOUT("remoteCallTimeout"),
    REMOTE_COMMAND_EXECUTOR("remote-command-executor"),
    REMOTE_SITE("remote-site"),
    REMOTE_TIMEOUT("remote-timeout"),
    REPLICATION_QUEUE_EXECUTOR("replication-queue-executor"),
    REPL_QUEUE_INTERVAL("replQueueInterval"),
    REPL_QUEUE_CLASS("replQueueClass"),
    REPL_QUEUE_MAX_ELEMENTS("replQueueMaxElements"),
    REPL_TIMEOUT("replTimeout"),
    RETRY_WAIT_TIME_INCREASE_FACTOR("retryWaitTimeIncreaseFactor"),
    ROLES("roles"),
    SEGMENTS("segments"),
    SHARED("shared"),
    SHUTDOWN_HOOK("shutdown-hook"),
    SHUTDOWN_TIMEOUT("shutdown-timeout"),
    SINGLETON("singleton"),
    SITE("site"),
    SPIN_DURATION("deadlock-detection-spin"),
    STATISTICS("statistics"),
    START("start"),
    STORE_KEYS_AS_BINARY("keys"),
    STORE_VALUES_AS_BINARY("values"),
    STRATEGY("strategy"),
    STREAM_BUFFER_SIZE("streamBufferSize"),
    STRICT_PEER_TO_PEER("strictPeerToPeer"),
    STRIPING("striping"),
    SUPPORTS_CONCURRENT_UPDATES("supportsConcurrentUpdates"),
    STACK("stack"),
    STOP_TIMEOUT("stop-timeout"),
    SYNC_COMMIT_PHASE("syncCommitPhase"),
    SYNC_ROLLBACK_PHASE("syncRollbackPhase"),
    TAKE_BACKUP_OFFLINE_AFTER_FAILURES("after-failures"),
    TAKE_BACKUP_OFFLINE_MIN_WAIT("min-wait"),
    THREAD_FACTORY("thread-factory"),
    THREAD_NAME_PATTERN("thread-name-pattern"),
    THREAD_POLICY("thread-policy"),
    THREAD_POOL_SIZE("thread-pool-size"),
    TIMEOUT("timeout"),
    TOTAL_ORDER_EXECUTOR("total-order-executor"),
    TRANSACTION_MANAGER_LOOKUP_CLASS("transaction-manager-lookup"),
    TRANSACTION_MODE("transactionMode"),
    TRANSACTION_PROTOCOL("protocol"),
    TRANSPORT_CLASS("transportClass"),
    TYPE_CONVERTER("typeConverter"),
    UNRELIABLE_RETURN_VALUES("unreliable-return-values"),
    USE_EAGER_LOCKING("useEagerLocking"),
    USE_LOCK_STRIPING("useLockStriping"),
    USE_REPL_QUEUE("useReplQueue"),
    USE_SYNCHRONIZAION("useSynchronization"),
    USE_TWO_PHASE_COMMIT("two-phase-commit"),
    USE_1PC_FOR_AUTOCOMMIT_TX("use1PcForAutoCommitTransactions"),
    VALUE("value"),
    VALUE_EQUIVALENCE("value-equivalence"),
    VERSION("version"),
    VERSIONING_SCHEME("scheme"),
    WAKE_UP_INTERVAL("wakeUpInterval"),
    WRITE_SKEW_CHECK("write-skew"),

//
//   ALIASES(ModelKeys.ALIASES),
//   APPEND_CACHE_NAME_TO_PATH(ModelKeys.APPEND_CACHE_NAME_TO_PATH),
//   ASYNC_MARSHALLING(ModelKeys.ASYNC_MARSHALLING),
//   AWAIT_INITIAL_TRANSFER(ModelKeys.AWAIT_INITIAL_TRANSFER),
//   BACKUP_FAILURE_POLICY(ModelKeys.BACKUP_FAILURE_POLICY),
//   BATCH_SIZE(ModelKeys.BATCH_SIZE),
//
//   BLOCK_SIZE(ModelKeys.BLOCK_SIZE),
//   BUFFER_SIZE(ModelKeys.BUFFER_SIZE),
//   CACHE(ModelKeys.CACHE),
//   CACHE_SIZE(ModelKeys.CACHE_SIZE),
//   CAPACITY_FACTOR(ModelKeys.CAPACITY_FACTOR),
//   CHUNK_SIZE(ModelKeys.CHUNK_SIZE),
//   CLASS(ModelKeys.CLASS),
//   CLEAR_THRESHOLD(ModelKeys.CLEAR_THRESHOLD),
//
//   CONCURRENCY_LEVEL(ModelKeys.CONCURRENCY_LEVEL),
//   CONNECTION_TIMEOUT(ModelKeys.CONNECTION_TIMEOUT),
//   DATASOURCE(ModelKeys.DATASOURCE),
//   DEFAULT_CACHE(ModelKeys.DEFAULT_CACHE),
//   @Deprecated DEFAULT_CACHE_CONTAINER("default-cache-container"),
//   @Deprecated EAGER_LOCKING("eager-locking"),
//   ENABLED(ModelKeys.ENABLED),

//
//   FETCH_SIZE(ModelKeys.FETCH_SIZE),
//
//   @Deprecated FLUSH_TIMEOUT("flush-timeout"),
//   HOTROD_WRAPPING(ModelKeys.HOTROD_WRAPPING),
//   INDEXING(ModelKeys.INDEXING),
//   INDEX(ModelKeys.INDEX),
//
//
//
//
//   LIFESPAN(ModelKeys.LIFESPAN),
//
//
//
//   MARSHALLER(ModelKeys.MARSHALLER),
//   MAX_CONNECTIONS_PER_HOST(ModelKeys.MAX_CONNECTIONS_PER_HOST),
//   MAX_ENTRIES(ModelKeys.MAX_ENTRIES),
//   MAX_IDLE(ModelKeys.MAX_IDLE),
//   MAX_TOTAL_CONNECTIONS(ModelKeys.MAX_TOTAL_CONNECTIONS),
//   MODE(ModelKeys.MODE),
//   MODIFICATION_QUEUE_SIZE(ModelKeys.MODIFICATION_QUEUE_SIZE),
//   NAME(ModelKeys.NAME),
//   NAMESPACE(XMLConstants.XMLNS_ATTRIBUTE),
//   OUTBOUND_SOCKET_BINDING(ModelKeys.OUTBOUND_SOCKET_BINDING),
//
//   PASSIVATION(ModelKeys.PASSIVATION),
//   PATH(ModelKeys.PATH),
//   PREFIX(ModelKeys.PREFIX),
//   PRELOAD(ModelKeys.PRELOAD),
//
//   RACK(ModelKeys.RACK),
//   RAW_VALUES(ModelKeys.RAW_VALUES),
//
//
//
//   SHARED(ModelKeys.SHARED),
//   SHUTDOWN_TIMEOUT(ModelKeys.SHUTDOWN_TIMEOUT),
//   SITE(ModelKeys.SITE),
//   SOCKET_TIMEOUT(ModelKeys.SOCKET_TIMEOUT),
//
//   STRICT_PEER_TO_PEER(ModelKeys.STRICT_PEER_TO_PEER),
//
//   STRATEGY(ModelKeys.STRATEGY),
//
//   TAKE_BACKUP_OFFLINE_AFTER_FAILURES(ModelKeys.TAKE_BACKUP_OFFLINE_AFTER_FAILURES),
//   TAKE_BACKUP_OFFLINE_MIN_WAIT(ModelKeys.TAKE_BACKUP_OFFLINE_MIN_WAIT),
//   TCP_NO_DELAY(ModelKeys.TCP_NO_DELAY),
//   THREAD_POOL_SIZE(ModelKeys.THREAD_POOL_SIZE),
//   TIMEOUT(ModelKeys.TIMEOUT),
//   TYPE(ModelKeys.TYPE),
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
