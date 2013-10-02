package org.infinispan.query.clustered;

import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;
import org.infinispan.query.impl.CommandInitializer;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.impl.ModuleCommandIds;

import java.util.UUID;

/**
 * Encapsulates all rpc calls for distributed queries actions
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredQueryCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.CLUSTERED_QUERY;
   private static final Integer ZERO = Integer.valueOf(0);

   private ClusteredQueryCommandType commandType;

   private HSQuery query;

   // local instance (set only when command arrives on target node)
   private Cache<?, ?> cache;

   // identifies the query
   private UUID lazyQueryId;

   // for retrieve keys on a lazy query
   private Integer docIndex = ZERO;

   private ClusteredQueryCommand(ClusteredQueryCommandType type, String cacheName) {
      super(cacheName);
      commandType = type;
   }

   /**
    * For CommandFactory only. To create a ClusteredQueryCommand, use createLazyIterator(),
    * destroyLazyQuery(), getResultSize() or retrieveKeyFromLazyQuery()
    */
   public ClusteredQueryCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public void fetchExecutionContext(CommandInitializer ci) {
      this.cache = ci.getCacheManager().getCache(cacheName);
   }

   public static ClusteredQueryCommand createLazyIterator(HSQuery query, Cache<?, ?> cache, UUID id) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_LAZY_ITERATOR, cache.getName());
      clQuery.query = query;
      clQuery.lazyQueryId = id;
      return clQuery;
   }

   public static ClusteredQueryCommand getResultSize(HSQuery query, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.GET_RESULT_SIZE, cache.getName());
      clQuery.query = query;
      return clQuery;
   }

   public static ClusteredQueryCommand createEagerIterator(HSQuery query, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_EAGER_ITERATOR, cache.getName());
      clQuery.query = query;
      return clQuery;
   }

   public static ClusteredQueryCommand destroyLazyQuery(Cache<?, ?> cache, UUID id) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.DESTROY_LAZY_ITERATOR, cache.getName());
      clQuery.lazyQueryId = id;
      return clQuery;
   }

   public static ClusteredQueryCommand retrieveKeyFromLazyQuery(Cache<?, ?> cache, UUID id, int docIndex) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.GET_SOME_KEYS, cache.getName());
      clQuery.lazyQueryId = id;
      clQuery.docIndex = docIndex;
      return clQuery;
   }

   public void initialize(Cache<?, ?> localInstance) {
      setCache(localInstance);
   }

   public void setCache(Cache<?, ?> cache) {
      this.cache = cache;
   }

   /**
    * Invokes a query on a (remote) cache and returns results (list of keys).
    * 
    * @param context
    *           invocation context, ignored.
    * @return returns an <code>List<Object></code>.
    */
   @Override
   public Object perform(InvocationContext context) throws Throwable {
      return perform(cache);
   }

   public QueryResponse perform(Cache<?, ?> cache) {
      ClusteredQueryCommandWorker worker = commandType.getCommand(cache, query, lazyQueryId, docIndex);
      return worker.perform();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[] { commandType, query, lazyQueryId, docIndex };
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      commandType = (ClusteredQueryCommandType) args[i++];
      query = (HSQuery) args[i++];
      lazyQueryId = (UUID) args[i++];
      docIndex = (Integer) args[i++];
   }

   @Override
   public String toString() {
      return "ClusteredQuery{ cache=" + getCacheName() + '}';
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((cacheName == null) ? 0 : cacheName.hashCode());
      result = prime * result + ((query == null) ? 0 : query.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof ClusteredQueryCommand))
         return false;
      ClusteredQueryCommand other = (ClusteredQueryCommand) obj;
      if (cacheName == null) {
         if (other.cacheName != null)
            return false;
      } else if (!cacheName.equals(other.cacheName))
         return false;
      if (query == null) {
         if (other.query != null)
            return false;
      } else if (!query.equals(other.query))
         return false;
      return true;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
