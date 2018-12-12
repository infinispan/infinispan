package org.infinispan.tools.store.migrator;

import org.infinispan.persistence.spi.MarshallableEntry;

public interface StoreIterator extends Iterable<MarshallableEntry>, AutoCloseable {
}
