package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.clustered.commandworkers.ClusteredQueryCommandWorker;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.util.ByteString;

/**
 * Encapsulates all rpc calls for distributed queries actions.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredQueryCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.CLUSTERED_QUERY;
   private static final Integer ZERO = Integer.valueOf(0);

   private ClusteredQueryCommandType commandType;

   private QueryDefinition queryDefinition;

   // local instance (set only when command arrives on target node)
   private Cache<?, ?> cache;

   // identifies the query
   private UUID lazyQueryId;

   // for retrieve keys on a lazy query
   private Integer docIndex = ZERO;

   private ClusteredQueryCommand(ClusteredQueryCommandType type, String cacheName) {
      super(ByteString.fromString(cacheName));
      commandType = type;
   }

   /**
    * For CommandFactory only. To create a ClusteredQueryCommand, use createLazyIterator(),
    * destroyLazyQuery(), getResultSize() or retrieveKeyFromLazyQuery()
    */
   public ClusteredQueryCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public void setCacheManager(EmbeddedCacheManager cm) {
      this.cache = cm.getCache(cacheName.toString());
   }

   public static ClusteredQueryCommand createLazyIterator(QueryDefinition queryDefinition, Cache<?, ?> cache, UUID id) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_LAZY_ITERATOR, cache.getName());
      clQuery.queryDefinition = queryDefinition;
      clQuery.lazyQueryId = id;
      return clQuery;
   }

   public static ClusteredQueryCommand getResultSize(QueryDefinition queryDefinition, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.GET_RESULT_SIZE, cache.getName());
      clQuery.queryDefinition = queryDefinition;
      return clQuery;
   }

   public static ClusteredQueryCommand createEagerIterator(QueryDefinition queryDefinition, Cache<?, ?> cache) {
      ClusteredQueryCommand clQuery = new ClusteredQueryCommand(ClusteredQueryCommandType.CREATE_EAGER_ITERATOR, cache.getName());
      clQuery.queryDefinition = queryDefinition;
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

   /**
    * Invokes a query on a (remote) cache and returns results (list of keys).
    *
    * @return returns a {@code CompletableFuture} with a {@code List<Object>}.
    */
   @Override
   public CompletableFuture<Object> invokeAsync() {
      return CompletableFuture.completedFuture(perform(cache));
   }

   public QueryResponse perform(Cache<?, ?> cache) {
      ClusteredQueryCommandWorker worker = commandType.getCommand(cache, queryDefinition, lazyQueryId, docIndex);
      return worker.perform();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(commandType, output);
      output.writeObject(queryDefinition);
      MarshallUtil.marshallUUID(lazyQueryId, output, true);
      output.writeInt(docIndex);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandType = MarshallUtil.unmarshallEnum(input, ClusteredQueryCommandType::valueOf);
      queryDefinition = (QueryDefinition) input.readObject();
      lazyQueryId = MarshallUtil.unmarshallUUID(input, true);
      docIndex = input.readInt();
   }

   @Override
   public String toString() {
      return "ClusteredQueryCommand{cache=" + getCacheName() + '}';
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((cacheName == null) ? 0 : cacheName.hashCode());
      result = prime * result + ((queryDefinition == null) ? 0 : queryDefinition.hashCode());
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
      if (queryDefinition == null) {
         if (other.queryDefinition != null)
            return false;
      } else if (!queryDefinition.equals(other.queryDefinition))
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
