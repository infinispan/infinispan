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

import javax.transaction.TransactionManager;
import java.util.Map;

/**
 * // TODO: navssurtani --> Document this
 *
 * @author Navin Surtani
 * @since 4.0
 */

public class QueryInterceptor extends CommandInterceptor {

   protected SearchFactoryImplementor searchFactory;
   protected TransactionManager transactionManager;

   @Inject
   public void init(SearchFactoryImplementor searchFactory, TransactionManager transactionManager) {

      log.debug("Entered QueryInterceptor.init()");

      this.searchFactory = searchFactory;
      this.transactionManager = transactionManager;
   }


   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {

      // This method will get the put() calls on the cache and then send them into Lucene once it's successful.

      log.debug("Entered the searchable core interceptor visitPutKeyValueCommand()");

      // do the actual put first.
      Object toReturn = invokeNextInterceptor(ctx, command);

      addToIndexes(command.getValue(), command.getKey().toString());

      return toReturn;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      log.debug("Entered the searchable core interceptor visitRemoveCommand()");

      // remove the object out of the cache first.
      Object valueRemoved = invokeNextInterceptor(ctx, command);

      System.out.println("Transaction Manager is " + transactionManager);

      if (command.isSuccessful()) {
         removeFromIndexes(valueRemoved, command.getKey().toString());
      }

      return valueRemoved;

   }


   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      log.debug("Entered the searchable core interceptor visitReplaceCommand()");

      Object valueReplaced = invokeNextInterceptor(ctx, command);
      if (valueReplaced != null) {

         Object[] parameters = command.getParameters();
         String keyString = command.getKey().toString();

         removeFromIndexes(parameters[1], keyString);
         addToIndexes(parameters[2], keyString);
      }

      return valueReplaced;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {

      log.debug("Entered searchable core interceptor visitPutMapCommand()");

      Object mapPut = invokeNextInterceptor(ctx, command);


      Map<Object, Object> dataMap = command.getMap();

      // Loop through all the keys and put those key, value pairings into lucene.

      for (Map.Entry entry : dataMap.entrySet()) {
         addToIndexes(entry.getValue(), entry.getKey().toString());
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

}
