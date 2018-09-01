package org.infinispan.query.clustered;

import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.query.clustered.commandworkers.CQCreateEagerQuery;
import org.infinispan.query.clustered.commandworkers.CQCreateLazyQuery;
import org.infinispan.query.clustered.commandworkers.CQGetResultSize;
import org.infinispan.query.clustered.commandworkers.CQKillLazyIterator;
import org.infinispan.query.clustered.commandworkers.CQLazyFetcher;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;
import org.infinispan.query.impl.QueryDefinition;

/**
 * Types of ClusteredQueryCommandWorker. Each type defines a different behavior for a
 * ClusteredQueryCommand...
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public enum ClusteredQueryCommandType {

   CREATE_LAZY_ITERATOR() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQCreateLazyQuery();
      }
   },
   CREATE_EAGER_ITERATOR() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQCreateEagerQuery();
      }
   },
   DESTROY_LAZY_ITERATOR() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQKillLazyIterator();
      }
   },
   GET_SOME_KEYS() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQLazyFetcher();
      }
   },
   GET_RESULT_SIZE() {
      @Override
      public ClusteredQueryCommandWorker getNewInstance() {
         return new CQGetResultSize();
      }
   };

   private static final ClusteredQueryCommandType[] CACHED_VALUES = values();

   protected abstract ClusteredQueryCommandWorker getNewInstance();

   public ClusteredQueryCommandWorker getCommand(Cache<?, ?> cache, QueryDefinition queryDefinition, UUID lazyQueryId,
                                                 int docIndex) {
      ClusteredQueryCommandWorker command = getNewInstance();
      command.init(cache, queryDefinition, lazyQueryId, docIndex);
      return command;
   }

   public static ClusteredQueryCommandType valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

}
