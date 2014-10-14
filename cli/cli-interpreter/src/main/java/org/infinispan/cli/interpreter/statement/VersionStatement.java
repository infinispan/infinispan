package org.infinispan.cli.interpreter.statement;

import org.infinispan.Version;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;

/**
 *
 * Implementation of the "version" statement
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class VersionStatement implements Statement {

   public VersionStatement() {
   }

   @Override
   public Result execute(Session session) {
      return new StringResult("Server Version "+ Version.class.getPackage().getImplementationVersion());
   }

}
