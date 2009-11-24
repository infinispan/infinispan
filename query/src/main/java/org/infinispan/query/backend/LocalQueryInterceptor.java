package org.infinispan.query.backend;

import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;

import javax.transaction.TransactionManager;

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

      if (log.isDebugEnabled()) log.debug("Entered LocalQueryInterceptor.init()");

      // Fields on superclass.

      this.searchFactory = searchFactory;
      this.transactionManager = transactionManager;
   }

   @Override
   protected boolean shouldModifyIndexes(InvocationContext ctx) {
      return ctx.isOriginLocal();   
   }
}
