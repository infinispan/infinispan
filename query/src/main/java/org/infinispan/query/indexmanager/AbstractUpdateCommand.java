package org.infinispan.query.indexmanager;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.util.ByteString;

/**
 * Base class for index commands.
 * <p>
 * This class is public so it can be used by other internal Infinispan packages but should not be considered part of a
 * public API.
 *
 * @author gustavonalle
 * @since 7.0
 */
public abstract class AbstractUpdateCommand extends BaseRpcCommand implements CustomQueryCommand {

   protected SearchIntegrator searchFactory;
   protected String indexName;
   protected QueryInterceptor queryInterceptor;
   private byte[] serializedLuceneWorks;

   protected AbstractUpdateCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public abstract CompletableFuture<Object> invokeAsync() throws Throwable;

   @Override
   public void writeTo(ObjectOutput output) throws IOException {
      MarshallUtil.marshallString(indexName, output);
      MarshallUtil.marshallByteArray(serializedLuceneWorks, output);
   }

   @Override
   public void readFrom(ObjectInput input) throws IOException {
      indexName = MarshallUtil.unmarshallString(input);
      serializedLuceneWorks = MarshallUtil.unmarshallByteArray(input);
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   /**
    * This is invoked only on the receiving node, before {@link #perform(org.infinispan.context.InvocationContext)}.
    */
   @Override
   public void setCacheManager(EmbeddedCacheManager cm) {
      String name = cacheName.toString();
      if (cm.cacheExists(name)) {
         Cache cache = cm.getCache(name);
         searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
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

   public void setIndexName(String indexName) {
      this.indexName = indexName;
   }

   public void setSerializedLuceneWorks(byte[] serializedLuceneWorks) {
      this.serializedLuceneWorks = serializedLuceneWorks;
   }

   /**
    * Get the {@link LuceneWork} objects sent to us by a remote party. This method can only be called after {@link
    * #setCacheManager} was invoked.
    *
    * @return the list of {@link LuceneWork}s
    */
   protected List<LuceneWork> getLuceneWorks() {
      return searchFactory.getWorkSerializer().toLuceneWorks(serializedLuceneWorks);
   }
}
