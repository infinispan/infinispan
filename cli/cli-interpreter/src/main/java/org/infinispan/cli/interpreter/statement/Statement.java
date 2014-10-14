package org.infinispan.cli.interpreter.statement;

import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.session.Session;

public interface Statement {
   Result execute(Session session) throws StatementException;
}
