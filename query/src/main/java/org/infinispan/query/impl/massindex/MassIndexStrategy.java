package org.infinispan.query.impl.massindex;

/**
 * Execution plan of a MassIndexer
 *
 * @author gustavonalle
 * @since 8.2
 */
interface MassIndexStrategy {

   enum CleanExecutionMode {ONCE_BEFORE, PER_NODE}

   enum FlushExecutionMode {PER_NODE, ONCE_AFTER}

   enum IndexingExecutionMode {PRIMARY_OWNER, ALL}

   FlushExecutionMode getFlushStrategy();

   CleanExecutionMode getCleanStrategy();

   IndexingExecutionMode getIndexingStrategy();

   /**
    * The shared strategy will purge the index from the calling node,
    * index only primary owners on each node in parallel, and will execute a
    * single flush in the end.
    */
   MassIndexStrategy SHARED_INDEX_STRATEGY = new MassIndexStrategy() {
      @Override
      public FlushExecutionMode getFlushStrategy() {
         return FlushExecutionMode.ONCE_AFTER;
      }

      @Override
      public CleanExecutionMode getCleanStrategy() {
         return CleanExecutionMode.ONCE_BEFORE;
      }

      @Override
      public IndexingExecutionMode getIndexingStrategy() {
         return IndexingExecutionMode.PRIMARY_OWNER;
      }
   };

   /**
    * The per node all data strategy will execute all the work on a per node basis,
    * including purging, indexing (not only primary owners) and flushing.
    */
   MassIndexStrategy PER_NODE_ALL_DATA = new MassIndexStrategy() {
      @Override
      public FlushExecutionMode getFlushStrategy() {
         return FlushExecutionMode.PER_NODE;
      }

      @Override
      public CleanExecutionMode getCleanStrategy() {
         return CleanExecutionMode.PER_NODE;
      }

      @Override
      public IndexingExecutionMode getIndexingStrategy() {
         return IndexingExecutionMode.ALL;
      }
   };


   /**
    * The per node primary strategy will execute all the work on a per node basis,
    * considering only primary owners when indexing.
    */
   MassIndexStrategy PER_NODE_PRIMARY = new MassIndexStrategy() {
      @Override
      public FlushExecutionMode getFlushStrategy() {
         return FlushExecutionMode.PER_NODE;
      }

      @Override
      public CleanExecutionMode getCleanStrategy() {
         return CleanExecutionMode.PER_NODE;
      }

      @Override
      public IndexingExecutionMode getIndexingStrategy() {
         return IndexingExecutionMode.PRIMARY_OWNER;
      }
   };


}
