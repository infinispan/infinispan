package org.infinispan.query.indexmanager;

import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.CommandInitializer;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for index commands
 * @author gustavonalle
 * @since 7.0
 */
public abstract class AbstractUpdateCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   protected static final Log log = LogFactory.getLog(AbstractUpdateCommand.class, Log.class);

   protected SearchFactoryImplementor searchFactory;
   protected String indexName;
   protected byte[] serializedModel;
   protected QueryInterceptor queryInterceptor;

   protected AbstractUpdateCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public abstract Object perform(InvocationContext ctx) throws Throwable;

   @Override
   public abstract byte getCommandId();

   @Override
   public Object[] getParameters() {
      return new Object[]{indexName, serializedModel};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      this.indexName = (String) parameters[0];
      this.serializedModel = (byte[]) parameters[1];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   /**
    * This is invoked only on the receiving node, before {@link #perform(InvocationContext)}
    */
   @Override
   public void fetchExecutionContext(CommandInitializer ci) {
      if (ci.getCacheManager().cacheExists(cacheName)) {
         Cache cache = ci.getCacheManager().getCache(cacheName);
         SearchManager searchManager = Search.getSearchManager(cache);
         searchFactory = (SearchFactoryImplementor) searchManager.getSearchFactory();
         queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      }
      else {
         throw new CacheException("Cache named '"+ cacheName + "' does not exist on this CacheManager, or was not started" );
      }
   }

   @Override
   public boolean canBlock() {
      return true;
   }
   
   protected List<LuceneWork> transformKeysToStrings(final List<LuceneWork> luceneWorks) {
      ArrayList<LuceneWork> transformedWorks = new ArrayList<>(luceneWorks.size());
      for (LuceneWork lw : luceneWorks) {
         transformedWorks.add(transformKeyToStrings(lw));
      }
      return transformedWorks;
   }

   protected LuceneWork transformKeyToStrings(final LuceneWork luceneWork) {
      final KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      return luceneWork.getWorkDelegate(LuceneWorkTransformationVisitor.INSTANCE).cloneOverridingIdString(luceneWork, keyTransformationHandler);
   }


   protected void setSerializedWorkList(byte[] serializedModel) {
      this.serializedModel = serializedModel;
   }

   protected void setIndexName(String indexName) {
      this.indexName = indexName;
   }
}
