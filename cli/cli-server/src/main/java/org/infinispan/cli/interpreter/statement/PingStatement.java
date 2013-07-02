package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.result.EmptyResult;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * PingStatement.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class PingStatement implements Statement {

   public PingStatement() {
   }

   @Override
   public Result execute(Session session) {
      return EmptyResult.RESULT;
   }

}
