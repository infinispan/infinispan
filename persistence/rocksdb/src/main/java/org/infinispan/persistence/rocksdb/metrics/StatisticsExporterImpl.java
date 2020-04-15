package org.infinispan.persistence.rocksdb.metrics;

import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

@MBean
public class StatisticsExporterImpl implements StatisticsExporter {

    private Statistics statistics;

    public void init(Statistics statistics) {
        this.statistics = statistics;
    }

    @ManagedAttribute
    public long getBlockCacheMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_MISS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_HIT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheAdd() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_ADD) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheAddFailures() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_ADD_FAILURES) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheIndexMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_MISS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheIndexHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_HIT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheIndexAdd() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_ADD) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheIndexBytesInsert() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_BYTES_INSERT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheIndexBytesEvict() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_INDEX_BYTES_EVICT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheFilterMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_MISS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheFilterHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_HIT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheFilterAdd() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_ADD) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheFilterBytesInsert() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_BYTES_INSERT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheFilterBytesEvict() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_FILTER_BYTES_EVICT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheDataMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_MISS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheDataHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_HIT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheDataAdd() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_ADD) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheDataBytesInsert() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_DATA_BYTES_INSERT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheBytesWrite() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_BYTES_WRITE) : 0;
    }

    @ManagedAttribute
    public long getBloomFilterUseful() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOOM_FILTER_USEFUL) : 0;
    }

    @ManagedAttribute
    public long getPersistentCacheHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.PERSISTENT_CACHE_HIT) : 0;
    }

    @ManagedAttribute
    public long getPersistentCacheMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.PERSISTENT_CACHE_MISS) : 0;
    }

    @ManagedAttribute
    public long getSimBlockCacheHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.SIM_BLOCK_CACHE_HIT) : 0;
    }

    @ManagedAttribute
    public long getSimBlockCacheMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.SIM_BLOCK_CACHE_MISS) : 0;
    }

    @ManagedAttribute
    public long getMemtableHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.MEMTABLE_HIT) : 0;
    }

    @ManagedAttribute
    public long getMemtableMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.MEMTABLE_MISS) : 0;
    }

    @ManagedAttribute
    public long getGetHitL0() {
        return statistics != null ? statistics.getTickerCount(TickerType.GET_HIT_L0) : 0;
    }

    @ManagedAttribute
    public long getGetHitL1() {
        return statistics != null ? statistics.getTickerCount(TickerType.GET_HIT_L1) : 0;
    }

    @ManagedAttribute
    public long getGetHitL2AndUp() {
        return statistics != null ? statistics.getTickerCount(TickerType.GET_HIT_L2_AND_UP) : 0;
    }

    @ManagedAttribute
    public long getCompactionKeyDropNewerEntry() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_NEWER_ENTRY) : 0;
    }

    @ManagedAttribute
    public long getCompactionKeyDropObsolete() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_OBSOLETE) : 0;
    }

    @ManagedAttribute
    public long getCompactionKeyDropRangeDel() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_RANGE_DEL) : 0;
    }

    @ManagedAttribute
    public long getCompactionKeyDropUser() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_KEY_DROP_USER) : 0;
    }

    @ManagedAttribute
    public long getCompactionRangeDelDropObsolete() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_RANGE_DEL_DROP_OBSOLETE) : 0;
    }

    @ManagedAttribute
    public long getNumberKeysWritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN) : 0;
    }

    @ManagedAttribute
    public long getNumberKeysRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_KEYS_READ) : 0;
    }

    @ManagedAttribute
    public long getNumberKeysUpdated() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_KEYS_UPDATED) : 0;
    }

    @ManagedAttribute
    public long getBytesWritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BYTES_WRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getNumberDbSeek() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_SEEK) : 0;
    }

    @ManagedAttribute
    public long getNumberDbNext() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_NEXT) : 0;
    }

    @ManagedAttribute
    public long getNumberDbPrev() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_PREV) : 0;
    }

    @ManagedAttribute
    public long getNumberDbSeekFound() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_SEEK_FOUND) : 0;
    }

    @ManagedAttribute
    public long getNumberDbNextFound() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_NEXT_FOUND) : 0;
    }

    @ManagedAttribute
    public long getNumberDbPrevFound() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DB_PREV_FOUND) : 0;
    }

    @ManagedAttribute
    public long getIterBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.ITER_BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getNoFileCloses() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_FILE_CLOSES) : 0;
    }

    @ManagedAttribute
    public long getNoFileOpens() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_FILE_OPENS) : 0;
    }

    @ManagedAttribute
    public long getNoFileErrors() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_FILE_ERRORS) : 0;
    }

    @ManagedAttribute
    public long getStallMicros() {
        return statistics != null ? statistics.getTickerCount(TickerType.STALL_MICROS) : 0;
    }

    @ManagedAttribute
    public long getDbMutexWaitMicros() {
        return statistics != null ? statistics.getTickerCount(TickerType.DB_MUTEX_WAIT_MICROS) : 0;
    }

    @ManagedAttribute
    public long getRateLimitDelayMillis() {
        return statistics != null ? statistics.getTickerCount(TickerType.RATE_LIMIT_DELAY_MILLIS) : 0;
    }

    @ManagedAttribute
    public long getNoIterators() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_ITERATORS) : 0;
    }

    @ManagedAttribute
    public long getNumberMultigetCalls() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_MULTIGET_CALLS) : 0;
    }

    @ManagedAttribute
    public long getNumberMultigetKeysRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ) : 0;
    }

    @ManagedAttribute
    public long getNumberMultigetBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_MULTIGET_BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getNumberFilteredDeletes() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_FILTERED_DELETES) : 0;
    }

    @ManagedAttribute
    public long getNumberMergeFailures() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_MERGE_FAILURES) : 0;
    }

    @ManagedAttribute
    public long getBloomFilterPrefixChecked() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOOM_FILTER_PREFIX_CHECKED) : 0;
    }

    @ManagedAttribute
    public long getBloomFilterPrefixUseful() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOOM_FILTER_PREFIX_USEFUL) : 0;
    }

    @ManagedAttribute
    public long getNumberOfReseeksInIteration() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_OF_RESEEKS_IN_ITERATION) : 0;
    }

    @ManagedAttribute
    public long getGetUpdatesSinceCalls() {
        return statistics != null ? statistics.getTickerCount(TickerType.GET_UPDATES_SINCE_CALLS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheCompressedMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSED_MISS) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheCompressedHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSED_HIT) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheCompressedAdd() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSED_ADD) : 0;
    }

    @ManagedAttribute
    public long getBlockCacheCompressedAddFailures() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOCK_CACHE_COMPRESSED_ADD_FAILURES) : 0;
    }

    @ManagedAttribute
    public long getWalFileSynced() {
        return statistics != null ? statistics.getTickerCount(TickerType.WAL_FILE_SYNCED) : 0;
    }

    @ManagedAttribute
    public long getWalFileBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.WAL_FILE_BYTES) : 0;
    }

    @ManagedAttribute
    public long getWriteDoneBySelf() {
        return statistics != null ? statistics.getTickerCount(TickerType.WRITE_DONE_BY_SELF) : 0;
    }

    @ManagedAttribute
    public long getWriteDoneByOther() {
        return statistics != null ? statistics.getTickerCount(TickerType.WRITE_DONE_BY_OTHER) : 0;
    }

    @ManagedAttribute
    public long getWriteTimedout() {
        return statistics != null ? statistics.getTickerCount(TickerType.WRITE_TIMEDOUT) : 0;
    }

    @ManagedAttribute
    public long getWriteWithWal() {
        return statistics != null ? statistics.getTickerCount(TickerType.WRITE_WITH_WAL) : 0;
    }

    @ManagedAttribute
    public long getCompactReadBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACT_READ_BYTES) : 0;
    }

    @ManagedAttribute
    public long getCompactWriteBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACT_WRITE_BYTES) : 0;
    }

    @ManagedAttribute
    public long getFlushWriteBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.FLUSH_WRITE_BYTES) : 0;
    }

    @ManagedAttribute
    public long getNumberDirectLoadTableProperties() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_DIRECT_LOAD_TABLE_PROPERTIES) : 0;
    }

    @ManagedAttribute
    public long getNumberSuperversionAcquires() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_SUPERVERSION_ACQUIRES) : 0;
    }

    @ManagedAttribute
    public long getNumberSuperversionReleases() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_SUPERVERSION_RELEASES) : 0;
    }

    @ManagedAttribute
    public long getNumberSuperversionCleanups() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_SUPERVERSION_CLEANUPS) : 0;
    }

    @ManagedAttribute
    public long getNumberBlockCompressed() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_BLOCK_COMPRESSED) : 0;
    }

    @ManagedAttribute
    public long getNumberBlockDecompressed() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_BLOCK_DECOMPRESSED) : 0;
    }

    @ManagedAttribute
    public long getNumberBlockNotCompressed() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_BLOCK_NOT_COMPRESSED) : 0;
    }

    @ManagedAttribute
    public long getMergeOperationTotalTime() {
        return statistics != null ? statistics.getTickerCount(TickerType.MERGE_OPERATION_TOTAL_TIME) : 0;
    }

    @ManagedAttribute
    public long getFilterOperationTotalTime() {
        return statistics != null ? statistics.getTickerCount(TickerType.FILTER_OPERATION_TOTAL_TIME) : 0;
    }

    @ManagedAttribute
    public long getRowCacheHit() {
        return statistics != null ? statistics.getTickerCount(TickerType.ROW_CACHE_HIT) : 0;
    }

    @ManagedAttribute
    public long getRowCacheMiss() {
        return statistics != null ? statistics.getTickerCount(TickerType.ROW_CACHE_MISS) : 0;
    }

    @ManagedAttribute
    public long getReadAmpEstimateUsefulBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.READ_AMP_ESTIMATE_USEFUL_BYTES) : 0;
    }

    @ManagedAttribute
    public long getReadAmpTotalReadBytes() {
        return statistics != null ? statistics.getTickerCount(TickerType.READ_AMP_TOTAL_READ_BYTES) : 0;
    }

    @ManagedAttribute
    public long getNumberRateLimiterDrains() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_RATE_LIMITER_DRAINS) : 0;
    }

    @ManagedAttribute
    public long getNumberIterSkip() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_ITER_SKIP) : 0;
    }

    @ManagedAttribute
    public long getNumberMultigetKeysFound() {
        return statistics != null ? statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_FOUND) : 0;
    }

    @ManagedAttribute
    public long getNoIteratorCreated() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_ITERATOR_CREATED) : 0;
    }

    @ManagedAttribute
    public long getNoIteratorDeleted() {
        return statistics != null ? statistics.getTickerCount(TickerType.NO_ITERATOR_DELETED) : 0;
    }

    @ManagedAttribute
    public long getCompactionOptimizedDelDropObsolete() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_OPTIMIZED_DEL_DROP_OBSOLETE) : 0;
    }

    @ManagedAttribute
    public long getCompactionCancelled() {
        return statistics != null ? statistics.getTickerCount(TickerType.COMPACTION_CANCELLED) : 0;
    }

    @ManagedAttribute
    public long getBloomFilterFullPositive() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOOM_FILTER_FULL_POSITIVE) : 0;
    }

    @ManagedAttribute
    public long getBloomFilterFullTruePositive() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOOM_FILTER_FULL_TRUE_POSITIVE) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumPut() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_PUT) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumWrite() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_WRITE) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumGet() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_GET) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumMultiget() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_MULTIGET) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumSeek() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_SEEK) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumNext() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_NEXT) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumPrev() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_PREV) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumKeysWritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_KEYS_WRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBlobDbNumKeysRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_NUM_KEYS_READ) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBytesWritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BYTES_WRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getBlobDbWriteInlined() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_WRITE_INLINED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbWriteInlinedTtl() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_WRITE_INLINED_TTL) : 0;
    }

    @ManagedAttribute
    public long getBlobDbWriteBlob() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_WRITE_BLOB) : 0;
    }

    @ManagedAttribute
    public long getBlobDbWriteBlobTtl() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_WRITE_BLOB_TTL) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobFileBytesWritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_FILE_BYTES_WRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobFileBytesRead() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_FILE_BYTES_READ) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobFileSynced() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_FILE_SYNCED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobIndexExpiredCount() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_INDEX_EXPIRED_COUNT) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobIndexExpiredSize() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_INDEX_EXPIRED_SIZE) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobIndexEvictedCount() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_INDEX_EVICTED_COUNT) : 0;
    }

    @ManagedAttribute
    public long getBlobDbBlobIndexEvictedSize() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_BLOB_INDEX_EVICTED_SIZE) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcNumFiles() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_NUM_FILES) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcNumNewFiles() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_NUM_NEW_FILES) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcFailures() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_FAILURES) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcNumKeysOverwritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_NUM_KEYS_OVERWRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcNumKeysExpired() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_NUM_KEYS_EXPIRED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcNumKeysRelocated() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_NUM_KEYS_RELOCATED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcBytesOverwritten() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_BYTES_OVERWRITTEN) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcBytesExpired() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_BYTES_EXPIRED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbGcBytesRelocated() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_GC_BYTES_RELOCATED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbFifoNumFilesEvicted() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_FIFO_NUM_FILES_EVICTED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbFifoNumKeysEvicted() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_FIFO_NUM_KEYS_EVICTED) : 0;
    }

    @ManagedAttribute
    public long getBlobDbFifoBytesEvicted() {
        return statistics != null ? statistics.getTickerCount(TickerType.BLOB_DB_FIFO_BYTES_EVICTED) : 0;
    }

    @ManagedAttribute
    public long getTxnPrepareMutexOverhead() {
        return statistics != null ? statistics.getTickerCount(TickerType.TXN_PREPARE_MUTEX_OVERHEAD) : 0;
    }

    @ManagedAttribute
    public long getTxnOldCommitMapMutexOverhead() {
        return statistics != null ? statistics.getTickerCount(TickerType.TXN_OLD_COMMIT_MAP_MUTEX_OVERHEAD) : 0;
    }

    @ManagedAttribute
    public long getTxnDuplicateKeyOverhead() {
        return statistics != null ? statistics.getTickerCount(TickerType.TXN_DUPLICATE_KEY_OVERHEAD) : 0;
    }

    @ManagedAttribute
    public long getTxnSnapshotMutexOverhead() {
        return statistics != null ? statistics.getTickerCount(TickerType.TXN_SNAPSHOT_MUTEX_OVERHEAD) : 0;
    }

    @ManagedAttribute
    public long getTxnGetTryAgain() {
        return statistics != null ? statistics.getTickerCount(TickerType.TXN_GET_TRY_AGAIN) : 0;
    }
}
