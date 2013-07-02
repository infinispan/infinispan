package org.infinispan.cli.interpreter.statement;

import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.session.Session;

public abstract class AbstractTransactionStatement implements Statement {
   final String cacheName;

   public AbstractTransactionStatement(final String cacheName) {
      this.cacheName = cacheName;
   }

   protected TransactionManager getTransactionManager(Session session) {
      Cache<Object, Object> cache = session.getCache(cacheName);
      return cache.getAdvancedCache().getTransactionManager();
   }
}
