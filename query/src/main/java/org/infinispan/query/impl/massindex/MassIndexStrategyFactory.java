package org.infinispan.query.impl.massindex;

import static org.infinispan.query.impl.massindex.MassIndexStrategy.PER_NODE_ALL_DATA;
import static org.infinispan.query.impl.massindex.MassIndexStrategy.SHARED_INDEX_STRATEGY;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.infinispan.query.impl.IndexInspector;

/**
 * @author gustavonalle
 * @since 8.2
 */
final class MassIndexStrategyFactory {

   private MassIndexStrategyFactory() {
   }

   static MassIndexStrategy calculateStrategy(IndexInspector indexInspector, IndexedTypeIdentifier typeIdentifier) {
      boolean sharedIndex = indexInspector.hasSharedIndex(typeIdentifier.getPojoType());
      return sharedIndex ? SHARED_INDEX_STRATEGY : PER_NODE_ALL_DATA;
   }
}
