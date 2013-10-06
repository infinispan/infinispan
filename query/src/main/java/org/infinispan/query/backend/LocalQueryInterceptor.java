package org.infinispan.query.backend;

import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

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

   private static final Log log = LogFactory.getLog(LocalQueryInterceptor.class, Log.class);

   public LocalQueryInterceptor(SearchFactoryIntegrator searchFactory) {
      super(searchFactory);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean shouldModifyIndexes(FlagAffectedCommand command, InvocationContext ctx) {
      // will index only local updates that were not flagged with SKIP_INDEXING,
      // are not caused internally by state transfer and indexing strategy is not configured to 'manual'
      return super.shouldModifyIndexes(command, ctx) && ctx.isOriginLocal() && !command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER);
   }
}
