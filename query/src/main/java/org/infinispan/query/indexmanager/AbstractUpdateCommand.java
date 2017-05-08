package org.infinispan.query.indexmanager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.CompletableFuture;

import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.CommandInitializer;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.impl.SearchManagerImpl;
import org.infinispan.query.logging.Log;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.LogFactory;

/**
 * Base class for index commands
 *
 * @author gustavonalle
 * @since 7.0
 */
public abstract class AbstractUpdateCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   protected static final Log log = LogFactory.getLog(AbstractUpdateCommand.class, Log.class);

   protected SearchIntegrator searchFactory;
   protected String indexName;
   protected byte[] serializedModel;
   protected QueryInterceptor queryInterceptor;

   protected AbstractUpdateCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public abstract CompletableFuture<Object> invokeAsync() throws Throwable;

   @Override
   public abstract byte getCommandId();

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      if (indexName == null) {
         output.writeBoolean(false);
      } else {
         output.writeBoolean(true);
         output.writeUTF(indexName);
      }
      MarshallUtil.marshallByteArray(serializedModel, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      boolean hasIndexName = input.readBoolean();
      if (hasIndexName) {
         indexName = input.readUTF();
      }
      serializedModel = MarshallUtil.unmarshallByteArray(input);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   /**
    * This is invoked only on the receiving node, before {@link #perform(org.infinispan.context.InvocationContext)}.
    */
   @Override
   public void fetchExecutionContext(CommandInitializer ci) {
      String name = cacheName.toString();
      if (ci.getCacheManager().cacheExists(name)) {
         Cache cache = ci.getCacheManager().getCache(name);
         SearchManager searchManager = new SearchManagerImpl(cache.getAdvancedCache());
         searchFactory = searchManager.unwrap(SearchIntegrator.class);
         queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      } else {
         throw new CacheException("Cache named '" + name + "' does not exist on this CacheManager, or was not started");
      }
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   public String getIndexName() {
      return indexName;
   }

   protected void setSerializedWorkList(byte[] serializedModel) {
      this.serializedModel = serializedModel;
   }

   public void setIndexName(String indexName) {
      this.indexName = indexName;
   }
}
