package org.infinispan.persistence.leveldb;

import org.infinispan.persistence.rocksdb.RocksDBStore;

/**
 * A backwards-compatible LevelDBStore backed by the RocksDBStore
 *
 * @author Tristan Tarrant
 * @deprecated Use the {@link RocksDBStore} instead
 */
@Deprecated
public class LevelDBStore extends RocksDBStore {
}
