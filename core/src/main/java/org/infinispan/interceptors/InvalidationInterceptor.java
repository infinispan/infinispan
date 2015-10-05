package org.infinispan.interceptors;

import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.base.SequentialInterceptor;
import org.infinispan.jmx.DelegatingJmxStatisticsExposer;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * This interceptor acts as a replacement to the replication interceptor when the CacheImpl is configured with
 * ClusteredSyncMode as INVALIDATE.
 * <p/>
 * The idea is that rather than replicating changes to all caches in a cluster when write methods are called, simply
 * broadcast an {@link InvalidateCommand} on the remote caches containing all keys modified.  This allows the remote
 * cache to look up the value in a shared cache loader which would have been updated with the changes.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 8.1, please use {@link org.infinispan.interceptors.sequential.InvalidationInterceptor} instead.
 */
@Deprecated
public class InvalidationInterceptor extends CommandInterceptor implements DelegatingJmxStatisticsExposer {
   private static final Log log = LogFactory.getLog(InvalidationInterceptor.class);
   private org.infinispan.interceptors.sequential.InvalidationInterceptor sequentialInterceptor;

   @Inject
   public void inject(org.infinispan.interceptors.sequential.InvalidationInterceptor sequentialInterceptor) {
      this.sequentialInterceptor = sequentialInterceptor;
   }

   @Override
   public Class<? extends SequentialInterceptor> getSequentialInterceptor() {
      return org.infinispan.interceptors.sequential.InvalidationInterceptor.class;
   }

   @Override
   public JmxStatisticsExposer getDelegate() {
      return sequentialInterceptor;
   }

   public long getInvalidations() {
      return sequentialInterceptor.getInvalidations();
   }
}
