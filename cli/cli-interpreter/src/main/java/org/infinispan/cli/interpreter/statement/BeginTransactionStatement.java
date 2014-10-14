package org.infinispan.cli.interpreter.statement;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * BeginTransactionStatement begins a transaction
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class BeginTransactionStatement extends AbstractTransactionStatement {
   private static final Log log = LogFactory.getLog(BeginTransactionStatement.class, Log.class);

   public BeginTransactionStatement(final String cacheName) {
      super(cacheName);
   }

   @Override
   public Result execute(Session session) throws StatementException {
      TransactionManager tm = getTransactionManager(session);
      if (tm==null) {
         throw log.noTransactionManager();
      }
      try {
         tm.begin();
         return EmptyResult.RESULT;
      } catch (NotSupportedException e) {
         throw log.noNestedTransactions();
      } catch (SystemException e) {
         throw log.unexpectedTransactionError(e);
      }
   }
}
