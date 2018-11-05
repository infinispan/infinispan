package org.infinispan.tools.store.migrator;

import org.infinispan.persistence.spi.MarshalledEntry;

public interface StoreIterator extends Iterable<MarshalledEntry>, AutoCloseable {
}
