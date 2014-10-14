package org.infinispan.cli.interpreter.statement;

import javax.transaction.TransactionManager;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.util.logging.LogFactory;

/**
 *
 * CommitTransactionStatement commits a running transaction
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class CommitTransactionStatement extends AbstractTransactionStatement {
   private static final Log log = LogFactory.getLog(CommitTransactionStatement.class, Log.class);

   public CommitTransactionStatement(final String cacheName) {
      super(cacheName);
   }

   @Override
   public Result execute(Session session) throws StatementException {
      TransactionManager tm = getTransactionManager(session);
      if (tm == null) {
         throw log.noTransactionManager();
      }
      try {
         tm.commit();
         return EmptyResult.RESULT;
      } catch (Exception e) {
         throw log.cannotCommitTransaction(e);
      }
   }

}
