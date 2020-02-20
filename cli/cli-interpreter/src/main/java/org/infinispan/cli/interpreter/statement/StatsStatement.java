package org.infinispan.cli.interpreter.statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.infinispan.AdvancedCache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.util.logging.LogFactory;

/**
 * Displays statistics about a container or a cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class StatsStatement implements Statement {
   private static final Log log = LogFactory.getLog(StatsStatement.class, Log.class);

   private enum Options {
      CONTAINER
   }

   final String cacheName;
   final private List<Option> options;

   public StatsStatement(List<Option> options, String cacheName) {
      this.options = options;
      this.cacheName = cacheName;
   }

   @Override
   public Result execute(Session session) throws StatementException {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);

      if (options.size() > 0) {
         for (Option option : options) {
            switch (option.toEnum(Options.class)) {
            case CONTAINER: {
               printContainerStats(pw, (DefaultCacheManager) session.getCacheManager());
               pw.flush();
               break;
            }
            }
         }
      } else {
         printCacheStats(pw, session.getCache(cacheName).getAdvancedCache());
      }

      pw.flush();
      return new StringResult(sw.toString());
   }

   private void printContainerStats(PrintWriter pw, DefaultCacheManager cacheManager) {
      pw.printf("%s: {\n", cacheManager.getClusterName());
      pw.printf("  status: %s\n", cacheManager.getCacheManagerStatus());
      pw.printf("  address: %s\n", cacheManager.getAddress());
      pw.printf("  physicalAddresses: %s\n", cacheManager.getPhysicalAddresses());
      pw.printf("  coordinator: %s\n", cacheManager.getCoordinator());
      pw.printf("  is coordinator? %s\n", cacheManager.isCoordinator());
      pw.printf("  clusterSize: %d\n", cacheManager.getClusterSize());
      pw.printf("  clusterMembers: %s\n", cacheManager.getClusterMembers());
      pw.printf("  clusterMembersPhysicalAddresses: %s\n", cacheManager.getClusterMembersPhysicalAddresses());
      pw.printf("  createdCacheCount: %s\n", cacheManager.getCreatedCacheCount());
      pw.printf("  definedCacheCount: %s\n", cacheManager.getDefinedCacheCount());
      pw.printf("  definedCacheNames: %s\n", cacheManager.getDefinedCacheNames());
      pw.printf("  version: %s\n", cacheManager.getVersion());
      pw.println("}");
   }

   private void printCacheStats(PrintWriter pw, AdvancedCache<?, ?> cache) throws StatementException {
      if (!cache.getCacheConfiguration().statistics().enabled()) {
         throw log.statisticsNotEnabled(cache.getName());
      }

      for (AsyncInterceptor interceptor : cache.getAsyncInterceptorChain().getInterceptors()) {
         printComponentStats(pw, cache, interceptor);
      }
      printComponentStats(pw, cache, cache.getLockManager());
      printComponentStats(pw, cache, cache.getRpcManager());
   }

   private void printComponentStats(PrintWriter pw, AdvancedCache<?, ?> cache, Object component) {
      if (component == null) {
         return;
      }
      BasicComponentRegistry bcr = cache.getComponentRegistry().getComponent(BasicComponentRegistry.class);
      MBeanMetadata mBeanMetadata = bcr.getMBeanMetadata(component.getClass().getName());
      if (mBeanMetadata == null) {
         return;
      }
      pw.printf("%s: {\n", mBeanMetadata.getJmxObjectName());
      List<MBeanMetadata.AttributeMetadata> attrs = new ArrayList<>(mBeanMetadata.getAttributes());
      attrs.sort(Comparator.comparing(MBeanMetadata.AttributeMetadata::getName));
      for (MBeanMetadata.AttributeMetadata s : attrs) {
         pw.printf("  %s: %s\n", s.getName(), getAttributeValue(component, s));
      }
      pw.println("}");
   }

   private Object getAttributeValue(Object o, MBeanMetadata.AttributeMetadata attr) {
      String name = attr.getName();
      String methodName = (attr.isIs() ? "is" : "get") + name.substring(0, 1).toUpperCase() + name.substring(1);
      try {
         Method method = o.getClass().getMethod(methodName);
         return method.invoke(o);
      } catch (Exception e) {
         return "N/A";
      }
   }
}
