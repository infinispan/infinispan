package org.infinispan.persistence.rocksdb.metrics;

public interface StatisticsExporter {
    long getBlockCacheMiss();

    long getBlockCacheHit();

    long getBlockCacheAdd();

    long getBlockCacheAddFailures();

    long getBlockCacheIndexMiss();

    long getBlockCacheIndexHit();

    long getBlockCacheIndexAdd();

    long getBlockCacheIndexBytesInsert();

    long getBlockCacheIndexBytesEvict();

    long getBlockCacheFilterMiss();

    long getBlockCacheFilterHit();

    long getBlockCacheFilterAdd();

    long getBlockCacheFilterBytesInsert();

    long getBlockCacheFilterBytesEvict();

    long getBlockCacheDataMiss();

    long getBlockCacheDataHit();

    long getBlockCacheDataAdd();

    long getBlockCacheDataBytesInsert();

    long getBlockCacheBytesRead();

    long getBlockCacheBytesWrite();

    long getBloomFilterUseful();

    long getPersistentCacheHit();

    long getPersistentCacheMiss();

    long getSimBlockCacheHit();

    long getSimBlockCacheMiss();

    long getMemtableHit();

    long getMemtableMiss();

    long getGetHitL0();

    long getGetHitL1();

    long getGetHitL2AndUp();

    long getCompactionKeyDropNewerEntry();

    long getCompactionKeyDropObsolete();

    long getCompactionKeyDropRangeDel();

    long getCompactionKeyDropUser();

    long getCompactionRangeDelDropObsolete();

    long getNumberKeysWritten();

    long getNumberKeysRead();

    long getNumberKeysUpdated();

    long getBytesWritten();

    long getBytesRead();

    long getNumberDbSeek();

    long getNumberDbNext();

    long getNumberDbPrev();

    long getNumberDbSeekFound();

    long getNumberDbNextFound();

    long getNumberDbPrevFound();

    long getIterBytesRead();

    long getNoFileCloses();

    long getNoFileOpens();

    long getNoFileErrors();

    long getStallMicros();

    long getDbMutexWaitMicros();

    long getRateLimitDelayMillis();

    long getNoIterators();

    long getNumberMultigetCalls();

    long getNumberMultigetKeysRead();

    long getNumberMultigetBytesRead();

    long getNumberFilteredDeletes();

    long getNumberMergeFailures();

    long getBloomFilterPrefixChecked();

    long getBloomFilterPrefixUseful();

    long getNumberOfReseeksInIteration();

    long getGetUpdatesSinceCalls();

    long getBlockCacheCompressedMiss();

    long getBlockCacheCompressedHit();

    long getBlockCacheCompressedAdd();

    long getBlockCacheCompressedAddFailures();

    long getWalFileSynced();

    long getWalFileBytes();

    long getWriteDoneBySelf();

    long getWriteDoneByOther();

    long getWriteTimedout();

    long getWriteWithWal();

    long getCompactReadBytes();

    long getCompactWriteBytes();

    long getFlushWriteBytes();

    long getNumberDirectLoadTableProperties();

    long getNumberSuperversionAcquires();

    long getNumberSuperversionReleases();

    long getNumberSuperversionCleanups();

    long getNumberBlockCompressed();

    long getNumberBlockDecompressed();

    long getNumberBlockNotCompressed();

    long getMergeOperationTotalTime();

    long getFilterOperationTotalTime();

    long getRowCacheHit();

    long getRowCacheMiss();

    long getReadAmpEstimateUsefulBytes();

    long getReadAmpTotalReadBytes();

    long getNumberRateLimiterDrains();

    long getNumberIterSkip();

    long getNumberMultigetKeysFound();

    long getNoIteratorCreated();

    long getNoIteratorDeleted();

    long getCompactionOptimizedDelDropObsolete();

    long getCompactionCancelled();

    long getBloomFilterFullPositive();

    long getBloomFilterFullTruePositive();

    long getBlobDbNumPut();

    long getBlobDbNumWrite();

    long getBlobDbNumGet();

    long getBlobDbNumMultiget();

    long getBlobDbNumSeek();

    long getBlobDbNumNext();

    long getBlobDbNumPrev();

    long getBlobDbNumKeysWritten();

    long getBlobDbNumKeysRead();

    long getBlobDbBytesWritten();

    long getBlobDbBytesRead();

    long getBlobDbWriteInlined();

    long getBlobDbWriteInlinedTtl();

    long getBlobDbWriteBlob();

    long getBlobDbWriteBlobTtl();

    long getBlobDbBlobFileBytesWritten();

    long getBlobDbBlobFileBytesRead();

    long getBlobDbBlobFileSynced();

    long getBlobDbBlobIndexExpiredCount();

    long getBlobDbBlobIndexExpiredSize();

    long getBlobDbBlobIndexEvictedCount();

    long getBlobDbBlobIndexEvictedSize();

    long getBlobDbGcNumFiles();

    long getBlobDbGcNumNewFiles();

    long getBlobDbGcFailures();

    long getBlobDbGcNumKeysOverwritten();

    long getBlobDbGcNumKeysExpired();

    long getBlobDbGcNumKeysRelocated();

    long getBlobDbGcBytesOverwritten();

    long getBlobDbGcBytesExpired();

    long getBlobDbGcBytesRelocated();

    long getBlobDbFifoNumFilesEvicted();

    long getBlobDbFifoNumKeysEvicted();

    long getBlobDbFifoBytesEvicted();

    long getTxnPrepareMutexOverhead();

    long getTxnOldCommitMapMutexOverhead();

    long getTxnDuplicateKeyOverhead();

    long getTxnSnapshotMutexOverhead();

    long getTxnGetTryAgain();
}
