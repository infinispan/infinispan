package org.infinispan.query.indexmanager;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hibernate.search.SearchException;
import org.hibernate.search.backend.LuceneWork;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.indexes.spi.IndexManager;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.impl.CommandInitializer;
import org.infinispan.query.impl.CustomQueryCommand;
import org.infinispan.query.impl.ModuleCommandIds;

/**
 * Custom RPC command containing an index update request for the
 * Master IndexManager of a specific cache & index.
 * 
* @author Sanne Grinovero
*/
public class IndexUpdateCommand extends BaseRpcCommand implements ReplicableCommand, CustomQueryCommand {

   public static final byte COMMAND_ID = ModuleCommandIds.UPDATE_INDEX;

   private SearchFactoryImplementor searchFactory;

   private byte[] serializedModel;

   private String indexName;

   private QueryInterceptor queryInterceptor;

   /**
    * Currently we need to ship this set as not all types
    * might be known to the master node.
    * TODO ISPN-2143
    */
   private Set<Class> knownIndexedTypes;

   public IndexUpdateCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      queryInterceptor.enableClasses(knownIndexedTypes);
      IndexManager indexManager = searchFactory.getIndexManagerHolder().getIndexManager(indexName);
      if (indexManager == null) {
         throw new SearchException("Unknown index referenced");
      }
      List<LuceneWork> luceneWorks = indexManager.getSerializer().toLuceneWorks(this.serializedModel);
      List<LuceneWork> workToApply = transformKeysToStrings(luceneWorks);//idInString field is not serialized, we need to extract it from the key object
      indexManager.performOperations(workToApply, null);
      return Boolean.TRUE; //Return value to be ignored
   }

   private List<LuceneWork> transformKeysToStrings(final List<LuceneWork> luceneWorks) {
      final KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      ArrayList<LuceneWork> transformedWorks = new ArrayList<LuceneWork>(luceneWorks.size());
      for (LuceneWork lw : luceneWorks) {
         LuceneWork transformedLuceneWork = lw
               .getWorkDelegate(LuceneWorkTransformationVisitor.INSTANCE)
               .cloneOverridingIdString(lw, keyTransformationHandler);
         transformedWorks.add(transformedLuceneWork);
      }
      return transformedWorks;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{ indexName, serializedModel, knownIndexedTypes };
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      this.indexName = (String) parameters[0];
      this.serializedModel = (byte[]) parameters[1];
      this.knownIndexedTypes = (Set<Class>) parameters[2];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean canBlock() {
      return true;
   }

   /**
    * This is invoked only on the receiving node, before {@link #perform(InvocationContext)}
    */
   @Override
   public void fetchExecutionContext(CommandInitializer ci) {
      this.searchFactory = ci.getSearchFactory();
      this.queryInterceptor = ci.getQueryInterceptor();
   }

   public void setSerializedWorkList(byte[] serializedModel) {
      this.serializedModel = serializedModel;
   }

   public void setIndexName(String indexName) {
      this.indexName = indexName;
   }

   public void setKnownIndexedTypes(Set<Class> knownIndexedTypes) {
      this.knownIndexedTypes = knownIndexedTypes;
   }

}
