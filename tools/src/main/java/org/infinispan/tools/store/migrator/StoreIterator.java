package org.infinispan.tools.store.migrator;

import org.infinispan.marshall.core.MarshalledEntry;

public interface StoreIterator extends Iterable<MarshalledEntry>, AutoCloseable {
}
