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

package org.jboss.as.clustering.infinispan.subsystem;

/**
 * @author Paul Ferraro
 * @author Tristan Tarrant
 */
public class ModelKeys {
    static final String CONFIGURATION_SUFFIX = "-configuration";

    static final String ACQUIRE_TIMEOUT = "acquire-timeout";
    static final String ALIAS = "alias";
    static final String ALIASES = "aliases";
    static final String APPEND_CACHE_NAME_TO_PATH = "append-cache-name-to-path";
    static final String ASYNC_EXECUTOR = "async-executor";
    @Deprecated
    static final String ASYNC_MARSHALLING = "async-marshalling";
    static final String AUDIT_LOGGER = "audit-logger";
    static final String AUTHORIZATION = "authorization";
    static final String AUTHORIZATION_NAME = "AUTHORIZATION";
    static final String AUTO_CONFIG = "auto-config";
    static final String AWAIT_INITIAL_TRANSFER = "await-initial-transfer";
    static final String BACKUP = "backup";
    static final String BACKUP_FAILURE_POLICY = "failure-policy";
    static final String BACKUP_STRATEGY = "strategy";
    static final String BACKUPS = "backups";
    static final String BACKUP_FOR = "backup-for";
    static final String BATCH_SIZE = "batch-size";
    static final String BATCHING = "batching";
    static final String BINARY_KEYED_TABLE = "binary-keyed-table";
    static final String BINARY_KEYED_TABLE_NAME = "BINARY_KEYED_TABLE";
    static final String BLOCK_SIZE = "block-size";
    static final String BUCKET_TABLE = "bucket-table";
    static final String BUFFER_SIZE = "buffer-size";
    static final String CACHE = "cache";
    static final String CACHE_AVAILABILITY = "cache-availability";
    static final String CACHE_SIZE = "cache-size";
    // static final String CACHE_MODE = "cache-mode";
    static final String CACHE_CONTAINER = "cache-container";
    static final String CACHE_REBALANCE = "cache-rebalance";
    static final String CACHE_REBALANCING_STATUS = "cache-rebalancing-status";
    static final String CHANNEL = "channel";
    static final String CHUNK_SIZE = "chunk-size";
    static final String CLASS = "class";
    static final String CLEAR_THRESHOLD = "clear-threshold";
    static final String CLUSTER = "cluster";
    static final String CLUSTER_LOADER = "cluster-loader";
    static final String CLUSTER_LOADER_NAME = "CLUSTER_LOADER";
    static final String CLUSTER_ROLE_MAPPER = "cluster-role-mapper";
    static final String COMMON_NAME_ROLE_MAPPER = "common-name-role-mapper";
    static final String COMPATIBILITY = "compatibility";
    static final String COMPATIBILITY_NAME = "COMPATIBILITY";
    static final String COMPRESSION = "compression";
    static final String COMPRESSION_NAME = "COMPRESSION";
    static final String CONCURRENCY_LEVEL = "concurrency-level";
    static final String CONFIGURATION = "configuration";
    static final String CONFIGURATIONS = "configurations";
    static final String CONFIGURATIONS_NAME = "CONFIGURATIONS";
    static final String CONNECTION_POOL = "connection-pool";
    static final String CONNECTION_TIMEOUT = "connection-timeout";
    static final String CREATE_ON_START = "create-on-start";
    static final String CUSTOM_ROLE_MAPPER = "custom-role-mapper";
    static final String DATA_COLUMN = "data-column";
    static final String DATASOURCE = "datasource";
    static final String DEFAULT_CACHE = "default-cache";
    static final String DIALECT = "dialect";
    static final String DISTRIBUTED_CACHE = "distributed-cache";
    static final String DISTRIBUTED_CACHE_CONFIGURATION = DISTRIBUTED_CACHE + CONFIGURATION_SUFFIX;
    static final String DROP_ON_EXIT = "drop-on-exit";
    static final String ENABLED = "enabled";
    static final String ENTRY_TABLE = "entry-table";
    static final String EVICTION = "eviction";
    static final String EVICTION_NAME = "EVICTION";
    @Deprecated
    static final String EVICTION_EXECUTOR = "eviction-executor";
    static final String EXPIRATION_EXECUTOR = "expiration-executor";
    static final String EXECUTOR = "executor";
    static final String EXPIRATION = "expiration";
    static final String EXPIRATION_NAME = "EXPIRATION";
    static final String FETCH_SIZE = "fetch-size";
    static final String FETCH_STATE = "fetch-state";
    static final String FILE_STORE = "file-store";
    static final String FILE_STORE_NAME = "FILE_STORE";
    static final String FLUSH_LOCK_TIMEOUT = "flush-lock-timeout";
    static final String GLOBAL_STATE = "global-state";
    static final String GLOBAL_STATE_NAME = "GLOBAL_STATE";
    static final String HOTROD_WRAPPING = "hotrod-wrapping";
    static final String ID_COLUMN = "id-column";
    static final String IDENTITY_ROLE_MAPPER = "identity-role-mapper";
    static final String IMPLEMENTATION = "implementation";
    static final String IMPLEMENTATION_NAME = "IMPLEMENTATION";
    static final String INDEX = "index";
    static final String INDEXING = "indexing";
    static final String INDEXING_PROPERTIES = "indexing-properties";
    static final String INLINE_INTERCEPTORS = "inline-interceptors";
    static final String INTERVAL = "interval";
    static final String INVALIDATION_CACHE = "invalidation-cache";
    static final String INVALIDATION_CACHE_CONFIGURATION = INVALIDATION_CACHE + CONFIGURATION_SUFFIX;
    static final String ISOLATION = "isolation";
    static final String JDBC_STORE = "jdbc-store";
    static final String JDBC_STORE_NAME = "JDBC_STORE";
    static final String KEEPALIVE_TIME = "keepalive-time";
    static final String LEVELDB_STORE = "leveldb-store";
    static final String LOADER = "loader";
    static final String LOADER_NAME = "LOADER";
    static final String STRING_KEYED_JDBC_STORE = "string-keyed-jdbc-store";
    static final String STRING_KEYED_JDBC_STORE_NAME = "STRING_KEYED_JDBC_STORE";
    static final String STRING_KEYED_TABLE = "string-keyed-table";
    static final String STRING_KEYED_TABLE_NAME = "STRING_KEYED_TABLE";
    static final String BINARY_KEYED_JDBC_STORE = "binary-keyed-jdbc-store";
    static final String BINARY_KEYED_JDBC_STORE_NAME = "BINARY_KEYED_JDBC_STORE";
    static final String MIXED_KEYED_JDBC_STORE = "mixed-keyed-jdbc-store";
    static final String MIXED_KEYED_JDBC_STORE_NAME = "MIXED_KEYED_JDBC_STORE";
    static final String JNDI_NAME = "jndi-name";
    static final String L1_LIFESPAN = "l1-lifespan";
    static final String LEVELDB_STORE_NAME = "LEVELDB_STORE";
    static final String LIFESPAN = "lifespan";
    static final String LISTENER_EXECUTOR = "listener-executor";
    static final String LOCAL_CACHE = "local-cache";
    static final String LOCAL_CACHE_CONFIGURATION = LOCAL_CACHE + CONFIGURATION_SUFFIX;
    static final String LOCK_TIMEOUT = "lock-timeout";
    static final String LOCKING = "locking";
    static final String LOCKING_NAME = "LOCKING";
    static final String MACHINE = "machine";
    static final String MAPPER = "mapper";
    static final String MARSHALLER = "marshaller";
    static final String MAX_CONNECTIONS_PER_HOST = "max-connections-per-host";
    static final String MAX_ENTRIES = "max-entries";
    static final String MAX_IDLE = "max-idle";
    static final String MAX_RETRIES = "max-retries";
    static final String MAX_THREADS = "max-threads";
    static final String MAX_TOTAL_CONNECTIONS = "max-total-connections";
    static final String MIGRATOR_NAME = "migrator-name";
    static final String MIN_THREADS = "min-threads";
    static final String MODE = "mode";
    static final String MODIFICATION_QUEUE_SIZE = "modification-queue-size";
    static final String MODULE = "module";
    static final String NAME = "name";
    static final String NOTIFICATIONS = "notifications";
    static final String OUTBOUND_SOCKET_BINDING = "outbound-socket-binding";
    static final String OWNERS = "owners";
    static final String PARTITION_HANDLING = "partition-handling";
    static final String PARTITION_HANDLING_NAME = "PARTITION_HANDLING";
    static final String PASSIVATION = "passivation";
    static final String PATH = "path";
    static final String PERMISSIONS = "permissions";
    static final String PERSISTENCE = "persistence";
    static final String PERSISTENT_LOCATION = "persistent-location";
    static final String PREFIX = "prefix";
    static final String PRELOAD = "preload";
    static final String PROPERTY = "property";
    static final String PROPERTIES = "properties";
    static final String PURGE = "purge";
    static final String QUEUE_FLUSH_INTERVAL = "queue-flush-interval";
    static final String QUEUE_LENGTH = "queue-length";
    static final String QUEUE_SIZE = "queue-size";
    static final String RACK = "rack";
    static final String RAW_VALUES = "raw-values";
    static final String READ_ONLY = "read-only";
    static final String REBALANCING = "rebalancing";
    static final String RELATIVE_TO = "relative-to";
    static final String REMOTE_CACHE = "remote-cache";
    static final String REMOTE_COMMAND_EXECUTOR = "remote-command-executor";
    static final String REMOTE_SERVER = "remote-server";
    static final String REMOTE_SERVERS = "remote-servers";
    static final String REMOTE_SITE = "remote-site";
    static final String REMOTE_STORE_NAME = "REMOTE_STORE";
    static final String REMOTE_STORE = "remote-store";
    static final String REMOTE_TIMEOUT = "remote-timeout";
    static final String REPLICATED_CACHE = "replicated-cache";
    static final String REPLICATED_CACHE_CONFIGURATION = REPLICATED_CACHE + CONFIGURATION_SUFFIX;
    static final String REPLICATION_QUEUE_EXECUTOR = "replication-queue-executor";
    static final String REST_STORE = "rest-store";
    static final String REST_STORE_NAME = "REST_STORE";
    static final String ROLE = "role";
    static final String ROLES = "roles";
    static final String SECURITY = "security";
    static final String SECURITY_NAME = "SECURITY";
    static final String SEGMENTS = "segments";
    static final String CAPACITY_FACTOR = "capacity-factor";
    static final String SHARED = "shared";
    static final String SHUTDOWN_TIMEOUT = "shutdown-timeout";
    static final String SIMPLE_CACHE = "simple-cache";
    static final String SINGLETON = "singleton";
    static final String SITE = "site";
    static final String SIZE = "size";
    static final String SOCKET_TIMEOUT = "socket-timeout";
    static final String STACK = "stack";
    static final String START = "start";
    static final String STATE_TRANSFER = "state-transfer";
    static final String STATE_TRANSFER_NAME = "STATE_TRANSFER";
    static final String STATE_TRANSFER_EXECUTOR = "state-transfer-executor";
    static final String STATISTICS = "statistics";
    static final String STATISTICS_AVAILABLE = "statistics-available";
    static final String STOP_TIMEOUT = "stop-timeout";
    static final String STORE = "store";
    static final String STORE_NAME = "STORE";
    static final String STRICT_PEER_TO_PEER = "strict-peer-to-peer";
    static final String STRATEGY = "strategy";
    static final String STRIPING = "striping";
    static final String TAKE_BACKUP_OFFLINE_AFTER_FAILURES = "after-failures";
    static final String TAKE_BACKUP_OFFLINE_MIN_WAIT = "min-wait";
    static final String TAKE_OFFLINE = "take-offline";
    static final String TCP_NO_DELAY = "tcp-no-delay";
    static final String TEMPORARY_LOCATION = "temporary-location";
    static final String THREAD_POOL_SIZE = "thread-pool-size";
    static final String TIMEOUT = "timeout";
    static final String TIMESTAMP_COLUMN = "timestamp-column";
    static final String TOTAL_ORDER_EXECUTOR = "total-order-executor";
    static final String TRANSACTION = "transaction";
    static final String TRANSACTION_NAME = "TRANSACTION";
    static final String TRANSPORT = "transport";
    static final String TRANSPORT_NAME = "TRANSPORT";
    static final String TYPE = "type";
    static final String TX_INTERNAL_ID = "internal-id";
    static final String WAIT = "wait";
    static final String WAIT_TIME = "wait-time";
    static final String WRITE_BEHIND = "write-behind";
    static final String WRITE_BEHIND_NAME = "WRITE_BEHIND";

    static final String ASYNC_OPERATIONS_THREAD_POOL = "async-operations-thread-pool";
    static final String LISTENER_THREAD_POOL = "listener-thread-pool";
    static final String EXPIRATION_THREAD_POOL = "expiration-thread-pool";
    static final String PERSISTENCE_THREAD_POOL = "persistence-thread-pool";
    static final String REMOTE_COMMAND_THREAD_POOL = "remote-command-thread-pool";
    static final String REPLICATION_QUEUE_THREAD_POOL = "replication-queue-thread-pool";
    static final String STATE_TRANSFER_THREAD_POOL = "state-transfer-thread-pool";
    static final String TRANSPORT_THREAD_POOL = "transport-thread-pool";
}
