package org.infinispan.query.backend;

import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;

import javax.transaction.TransactionManager;
import java.util.Map;

/**
 * <p/>
 * This class is an interceptor that will index data only if it has come from a local source.
 * <p/>
 * Currently, this is a property that is determined by setting "infinispan.query.indexLocalOnly" as a System property to
 * "true".
 *
 * @author Navin Surtani
 * @since 4.0
 */


public class LocalQueryInterceptor extends QueryInterceptor {

   @Inject
   public void init(SearchFactoryImplementor searchFactory, TransactionManager transactionManager) {

      log.debug("Entered LocalQueryInterceptor.init()");

      // Fields on superclass.

      this.searchFactory = searchFactory;
      this.transactionManager = transactionManager;
   }


   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {

      // This method will get the put() calls on the cache and then send them into Lucene once it's successful.

      log.debug("Entered the LocalQueryInterceptor visitPutKeyValueCommand()");

      // do the actual put first.
      Object toReturn = invokeNextInterceptor(ctx, command);

      // Since this is going to index if local only, then we must first check to see if the
      // context is local.

      if (ctx.isOriginLocal()) {
         log.debug("Origin is local");
         addToIndexes(command.getValue(), command.getKey().toString());
      }

      return toReturn;
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      log.debug("Entered the LocalQueryInterceptor visitRemoveCommand()");

      // remove the object out of the cache first.
      Object valueRemoved = invokeNextInterceptor(ctx, command);

      // Check to make sure that the context is local as well as successful.

      if (command.isSuccessful() && ctx.isOriginLocal()) {
         log.debug("Origin is local");
         removeFromIndexes(valueRemoved, command.getKey().toString());
      }

      return valueRemoved;

   }


   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      log.debug("Entered the LocalQueryInterceptor visitReplaceCommand()");

      Object valueReplaced = invokeNextInterceptor(ctx, command);

      // Checking for local as well as making sure that the valueReplaced is not null.

      if (valueReplaced != null && ctx.isOriginLocal()) {

         log.debug("Origin is local");
         Object[] parameters = command.getParameters();
         String keyString = command.getKey().toString();

         removeFromIndexes(parameters[1], keyString);
         addToIndexes(parameters[2], keyString);
      }

      return valueReplaced;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {

      log.debug("Entered LocalQueryInterceptor visitPutMapCommand()");

      Object mapPut = invokeNextInterceptor(ctx, command);

      // Check to make sure the origin is local.

      if (ctx.isOriginLocal()) {

         log.debug("Origin is local");
         Map<Object, Object> dataMap = command.getMap();

         // Loop through all the keys and put those key, value pairings into lucene.

         for (Map.Entry entry : dataMap.entrySet()) {
            addToIndexes(entry.getValue(), entry.getKey().toString());
         }
      }

      return mapPut;
   }


}
