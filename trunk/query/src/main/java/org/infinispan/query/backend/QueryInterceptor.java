package org.infinispan.query.backend;

import org.hibernate.search.backend.TransactionContext;
import org.hibernate.search.backend.Work;
import org.hibernate.search.backend.WorkType;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.marshall.MarshalledValue;

import javax.transaction.TransactionManager;
import java.util.Map;

/**
 * This interceptor will be created when the System Property "infinispan.query.indexLocalOnly" is "false"
 * <p/>
 * This type of interceptor will allow the indexing of data even when it comes from other caches within a cluster.
 * <p/>
 * However, if the a cache would not be putting the data locally, the interceptor will not index it.
 *
 * @author Navin Surtani
 * @since 4.0
 */

public class QueryInterceptor extends CommandInterceptor {

   protected SearchFactoryImplementor searchFactory;
   protected TransactionManager transactionManager;

   @Inject
   public void init(SearchFactoryImplementor searchFactory, TransactionManager transactionManager) {

      if (log.isDebugEnabled()) log.debug("Entered QueryInterceptor.init()");

      this.searchFactory = searchFactory;
      this.transactionManager = transactionManager;
   }


   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {

      // This method will get the put() calls on the cache and then send them into Lucene once it's successful.

      if (log.isDebugEnabled()) log.debug("Entered the searchable core interceptor visitPutKeyValueCommand()");

      // do the actual put first.
      Object toReturn = invokeNextInterceptor(ctx, command);

      addToIndexes(checkForMarshalledValue(command.getValue()), checkForMarshalledValue(command.getKey()).toString());

      return toReturn;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      if (log.isDebugEnabled()) log.debug("Entered the searchable core interceptor visitRemoveCommand()");

      // remove the object out of the cache first.
      Object valueRemoved = invokeNextInterceptor(ctx, command);

      if (log.isDebugEnabled()) log.debug("Transaction Manager is " + transactionManager);

      if (command.isSuccessful()) {
         removeFromIndexes(checkForMarshalledValue(valueRemoved), checkForMarshalledValue(command.getKey()).toString());
      }

      return valueRemoved;

   }


   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {

      if (log.isDebugEnabled()) log.debug("Entered the searchable core interceptor visitReplaceCommand()");

      Object valueReplaced = invokeNextInterceptor(ctx, command);
      if (valueReplaced != null) {

         Object[] parameters = command.getParameters();
         String keyString = checkForMarshalledValue(command.getKey()).toString();

         removeFromIndexes(checkForMarshalledValue(parameters[1]), keyString);
         addToIndexes(checkForMarshalledValue(parameters[2]), keyString);
      }

      return valueReplaced;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {

      if (log.isDebugEnabled()) log.debug("Entered searchable core interceptor visitPutMapCommand()");

      Object mapPut = invokeNextInterceptor(ctx, command);


      Map<Object, Object> dataMap = command.getMap();

      // Loop through all the keys and put those key, value pairings into lucene.

      for (Map.Entry entry : dataMap.entrySet()) {
         addToIndexes(checkForMarshalledValue(entry.getValue()), checkForMarshalledValue(entry.getKey()).toString());
      }
      return mapPut;
   }


   // Method that will be called when data needs to be added into Lucene.
   protected void addToIndexes(Object value, String key) {

      // The key here is the String representation of the key that is stored in the cache.
      // The key is going to be the documentID for Lucene.
      // The object parameter is the actual value that needs to be put into lucene.

      TransactionContext transactionContext = new TransactionalEventTransactionContext(transactionManager);
      searchFactory.getWorker().performWork(new Work(value, key, WorkType.ADD), transactionContext);
   }


   // Method that will be called when data needs to be removed from Lucene.
   protected void removeFromIndexes(Object value, String key) {

      // The key here is the String representation of the key that is stored in the cache.
      // The key is going to be the documentID for Lucene.
      // The object parameter is the actual value that needs to be removed from lucene.

      TransactionContext transactionContext = new TransactionalEventTransactionContext(transactionManager);
      searchFactory.getWorker().performWork(new Work(value, key, WorkType.DELETE), transactionContext);
   }

   // Check to see if a given object is a marshalled value or not.
   protected Object checkForMarshalledValue(Object o) {
      if (o instanceof MarshalledValue) {
         return ((MarshalledValue) o).get();
      } else {
         return o;
      }
   }

}
