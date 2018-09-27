package org.infinispan.query.clustered;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.clustered.commandworkers.CQCommandType;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.impl.ModuleCommandIds;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.util.ByteString;

/**
 * Encapsulates all rpc calls for distributed queries actions.
 * <p>
 * This class is public so it can be used by other internal Infinispan packages but should not be considered part of a
 * public API.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class ClusteredQueryCommand extends BaseRpcCommand implements CustomQueryCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.CLUSTERED_QUERY;

   private CQCommandType commandType;

   private QueryDefinition queryDefinition;

   // local instance (set only when command arrives on target node)
   private AdvancedCache<?, ?> cache;

   // identifies the query
   private UUID queryId;

   // for retrieve keys on a lazy query
   private int docIndex = 0;

   private ClusteredQueryCommand(CQCommandType commandType, String cacheName) {
      super(ByteString.fromString(cacheName));
      this.commandType = commandType;
   }

   /**
    * For CommandFactory only. To create a ClusteredQueryCommand, use createLazyIterator(), destroyLazyQuery(),
    * getResultSize() or retrieveKeyFromLazyQuery()
    */
   public ClusteredQueryCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public void setCacheManager(EmbeddedCacheManager cm) {
      this.cache = cm.getCache(cacheName.toString()).getAdvancedCache();
   }

   static ClusteredQueryCommand createLazyIterator(QueryDefinition queryDefinition, Cache<?, ?> cache, UUID queryId) {
      ClusteredQueryCommand cmd = new ClusteredQueryCommand(CQCommandType.CREATE_LAZY_ITERATOR, cache.getName());
      cmd.queryDefinition = queryDefinition;
      cmd.queryId = queryId;
      return cmd;
   }

   static ClusteredQueryCommand getResultSize(QueryDefinition queryDefinition, Cache<?, ?> cache) {
      ClusteredQueryCommand cmd = new ClusteredQueryCommand(CQCommandType.GET_RESULT_SIZE, cache.getName());
      cmd.queryDefinition = queryDefinition;
      return cmd;
   }

   static ClusteredQueryCommand createEagerIterator(QueryDefinition queryDefinition, Cache<?, ?> cache) {
      ClusteredQueryCommand cmd = new ClusteredQueryCommand(CQCommandType.CREATE_EAGER_ITERATOR, cache.getName());
      cmd.queryDefinition = queryDefinition;
      return cmd;
   }

   static ClusteredQueryCommand destroyLazyQuery(Cache<?, ?> cache, UUID queryId) {
      ClusteredQueryCommand cmd = new ClusteredQueryCommand(CQCommandType.DESTROY_LAZY_ITERATOR, cache.getName());
      cmd.queryId = queryId;
      return cmd;
   }

   static ClusteredQueryCommand retrieveKeyFromLazyQuery(Cache<?, ?> cache, UUID queryId, int docIndex) {
      ClusteredQueryCommand cmd = new ClusteredQueryCommand(CQCommandType.GET_SOME_KEYS, cache.getName());
      cmd.queryId = queryId;
      cmd.docIndex = docIndex;
      return cmd;
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

   public QueryResponse perform(AdvancedCache<?, ?> cache) {
      return commandType.perform(cache, queryDefinition, queryId, docIndex);
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallEnum(commandType, output);
      output.writeObject(queryDefinition);
      MarshallUtil.marshallUUID(queryId, output, true);
      output.writeInt(docIndex);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      commandType = MarshallUtil.unmarshallEnum(input, CQCommandType::valueOf);
      queryDefinition = (QueryDefinition) input.readObject();
      queryId = MarshallUtil.unmarshallUUID(input, true);
      docIndex = input.readInt();
   }

   @Override
   public String toString() {
      return "ClusteredQueryCommand{cache=" + getCacheName() + '}';
   }

   @Override
   public int hashCode() {
      int result = 31 + (cacheName == null ? 0 : cacheName.hashCode());
      return 31 * result + (queryDefinition == null ? 0 : queryDefinition.hashCode());
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj.getClass() != ClusteredQueryCommand.class)
         return false;
      ClusteredQueryCommand other = (ClusteredQueryCommand) obj;
      return (cacheName == null ? other.cacheName == null : cacheName.equals(other.cacheName)) &&
            (queryDefinition == null ? other.queryDefinition == null : queryDefinition.equals(other.queryDefinition));
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
