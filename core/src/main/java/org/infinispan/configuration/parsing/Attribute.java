/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

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

    AFTER("after"),
    ALLOW_DUPLICATE_DOMAINS("allowDuplicateDomains"),
    ALWAYS_PROVIDE_IN_MEMORY_STATE("alwaysProvideInMemoryState"),
    ASYNC_MARSHALLING("asyncMarshalling"),
    AUTO_COMMIT("autoCommit"),
    BEFORE("before"),
    CACHE_MANAGER_NAME("cacheManagerName"),
    CACHE_STOP_TIMEOUT("cacheStopTimeout"),
    CHUNK_SIZE("chunkSize"),
    CLASS("class"),
    CLUSTER_NAME("clusterName"),
    CONCURRENCY_LEVEL("concurrencyLevel"),
    DISTRIBUTED_SYNC_TIMEOUT("distributedSyncTimeout"),
    EAGER_LOCK_SINGLE_NODE("eagerLockSingleNode"),
    ENABLED("enabled"),
    EXTERNALIZER_CLASS("externalizerClass"),
    FACTORY("factory"),
    FETCH_IN_MEMORY_STATE("fetchInMemoryState"),
    FETCH_PERSISTENT_STATE("fetchPersistentState"),
    AWAIT_INITIAL_TRANSFER("awaitInitialTransfer"),
    FLUSH_LOCK_TIMEOUT("flushLockTimeout"),
    FSYNC_INTERVAL("fsyncInterval"),
    FSYNC_MODE("fsyncMode"),
    HASH_FUNCTION_CLASS("hashFunctionClass"),
    HASH_SEED_CLASS("hashSeedClass"),
    HOOK_BEHAVIOR("hookBehavior"),
    ID("id"),
    IGNORE_MODIFICATIONS("ignoreModifications"),
    INDEX("index"),
    INDEX_LOCAL_ONLY("indexLocalOnly"),
    INITIAL_RETRY_WAIT_TIME("initialRetryWaitTime"),
    INVALIDATION_THRESHOLD("invalidationThreshold"),
    ISOLATION_LEVEL("isolationLevel"),
    JMX_DOMAIN("jmxDomain"),
    LIFESPAN("lifespan"),
    LOCATION("location"),
    INVALIDATION_CLEANUP_TASK_FREQUENCY("cleanupTaskFrequency"),
    LOCK_ACQUISITION_TIMEOUT("lockAcquisitionTimeout"),
    LOCKING_MODE("lockingMode"),
    LOG_FLUSH_TIMEOUT("logFlushTimeout"),
    MACHINE_ID("machineId"),
    MARSHALLER_CLASS("marshallerClass"),
    MAX_ENTRIES("maxEntries"),
    MAX_IDLE("maxIdle"),
    MAX_NON_PROGRESSING_LOG_WRITES("maxProgressingLogWrites"),
    MBEAN_SERVER_LOOKUP("mBeanServerLookup"),
    MODE("mode"),
    NODE_NAME("nodeName"),
    MODIFICATION_QUEUE_SIZE("modificationQueueSize"),
    NAME("name"),
    NUM_OWNERS("numOwners"),
    NUM_SEGMENTS("numSegments"),
    NUM_RETRIES("numRetries"),
    NUM_VIRTUAL_NODES("numVirtualNodes"),
    ON_REHASH("onRehash"),
    PASSIVATION("passivation"),
    POSITION("position"),
    PRELOAD("preload"),
    PURGE_ON_STARTUP("purgeOnStartup"),
    PURGE_SYNCHRONOUSLY("purgeSynchronously"),
    PURGER_THREADS("purgerThreads"),
    PUSH_STATE_TIMEOUT("pushStateTimeout"),
    PUSH_STATE_WHEN_COORDINATOR("pushStateWhenCoordinator"),
    RACK_ID("rackId"),
    REAPER_ENABLED("reaperEnabled"),
    RECOVERY_INFO_CACHE_NAME("recoveryInfoCacheName"),
    REHASH_ENABLED("rehashEnabled"),
    REHASH_RPC_TIMEOUT("rehashRpcTimeout"),
    REHASH_WAIT("rehashWait"),
    REMOTE_CALL_TIMEOUT("remoteCallTimeout"),
    REPL_QUEUE_INTERVAL("replQueueInterval"),
    REPL_QUEUE_CLASS("replQueueClass"),
    REPL_QUEUE_MAX_ELEMENTS("replQueueMaxElements"),
    REPL_TIMEOUT("replTimeout"),
    RETRY_WAIT_TIME_INCREASE_FACTOR("retryWaitTimeIncreaseFactor"),
    SHARED("shared"),
    SHUTDOWN_TIMEOUT("shutdownTimeout"),
    SITE_ID("siteId"),
    SPIN_DURATION("spinDuration"),
    STORE_KEYS_AS_BINARY("storeKeysAsBinary"),
    STORE_VALUES_AS_BINARY("storeValuesAsBinary"),
    DEFENSIVE("defensive"),
    STRATEGY("strategy"),
    STREAM_BUFFER_SIZE("streamBufferSize"),
    SYNC_COMMIT_PHASE("syncCommitPhase"),
    SYNC_ROLLBACK_PHASE("syncRollbackPhase"),
    STRICT_PEER_TO_PEER("strictPeerToPeer"),
    THREAD_POLICY("threadPolicy"),
    THREAD_POOL_SIZE("threadPoolSize"),
    TIMEOUT("timeout"),
    TRANSACTION_MANAGER_LOOKUP_CLASS("transactionManagerLookupClass"),
    TRANSACTION_MODE("transactionMode"),
    TRANSPORT_CLASS("transportClass"),
    UNRELIABLE_RETURN_VALUES("unreliableReturnValues"),
    USE_EAGER_LOCKING("useEagerLocking"),
    USE_LOCK_STRIPING("useLockStriping"),
    SUPPORTS_CONCURRENT_UPDATES("supportsConcurrentUpdates"),
    USE_REPL_QUEUE("useReplQueue"),
    USE_SYNCHRONIZAION("useSynchronization"),
    VALUE("value"),
    VERSION("version"),
    WAKE_UP_INTERVAL("wakeUpInterval"),
    WRITE_SKEW_CHECK("writeSkewCheck"),
    USE_1PC_FOR_AUTOCOMMIT_TX("use1PcForAutoCommitTransactions"),
    VERSIONING_SCHEME("versioningScheme"),
    SITE("site"),
    BACKUP_FAILURE_POLICY("backupFailurePolicy"),
    REMOTE_SITE("remoteSite"),
    REMOTE_CACHE("remoteCache"),
    FAILURE_POLICY_CLASS("failurePolicyClass"),
    BACKUP_SITES("backupSites"),
    AFTER_FAILURES("afterFailures"),
    MIN_TIME_TO_WAIT("minTimeToWait"),
    USE_TWO_PHASE_COMMIT("useTwoPhaseCommit"),
    REAPER_WAKE_UP_INTERVAL("reaperWakeUpInterval"),
    COMPLETED_TX_TIMEOUT("completedTxTimeout"),
    TRANSACTION_PROTOCOL("transactionProtocol"),
    COMPARING_KEY("comparingKey"),
    COMPARING_VALUE("comparingValue");

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
