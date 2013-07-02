package org.infinispan.cli.interpreter.statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.cli.interpreter.result.Result;
import org.infinispan.cli.interpreter.result.StatementException;
import org.infinispan.cli.interpreter.result.StringResult;
import org.infinispan.cli.interpreter.session.Session;
import org.infinispan.factories.components.ComponentMetadata;
import org.infinispan.factories.components.ComponentMetadataRepo;
import org.infinispan.factories.components.JmxAttributeMetadata;
import org.infinispan.factories.components.ManageableComponentMetadata;
import org.infinispan.interceptors.base.CommandInterceptor;
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
   };

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
         printCacheStats(pw, session.getCache(cacheName));
      }

      pw.flush();
      return new StringResult(sw.toString());
   }

   private void printContainerStats(PrintWriter pw, DefaultCacheManager cacheManager) {
      pw.printf("%s: {\n", cacheManager.getClusterName());
      pw.printf("  status: %s\n", cacheManager.getCacheManagerStatus());
      pw.printf("  address: %s\n", cacheManager.getAddress());
      pw.printf("  coordinator: %s\n", cacheManager.getCoordinator());
      pw.printf("  clusterSize: %d\n", cacheManager.getClusterSize());
      pw.printf("  clusterMembers: %s\n", cacheManager.getClusterMembers());
      pw.printf("  createdCacheCount: %s\n", cacheManager.getCreatedCacheCount());
      pw.printf("  definedCacheCount: %s\n", cacheManager.getDefinedCacheCount());
      pw.printf("  definedCacheNames: %s\n", cacheManager.getDefinedCacheNames());
      pw.printf("  version: %s\n", cacheManager.getVersion());
      pw.println("}");
   }

   private void printCacheStats(PrintWriter pw, Cache<?, ?> cache) throws StatementException {
      if (!cache.getCacheConfiguration().jmxStatistics().enabled()) {
         throw log.statisticsNotEnabled(cache.getName());
      }

      for (CommandInterceptor interceptor : cache.getAdvancedCache().getInterceptorChain()) {
         printComponentStats(pw, cache, interceptor);
      }
      printComponentStats(pw, cache, cache.getAdvancedCache().getLockManager());
      printComponentStats(pw, cache, cache.getAdvancedCache().getRpcManager());
   }

   private void printComponentStats(PrintWriter pw, Cache<?, ?> cache, Object component) {
      if (component == null) {
         return;
      }
      ComponentMetadataRepo mr = cache.getAdvancedCache().getComponentRegistry().getComponentMetadataRepo();
      ComponentMetadata cm = mr.findComponentMetadata(component.getClass().getName());
      if (cm == null || !(cm instanceof ManageableComponentMetadata)) {
         return;
      }
      ManageableComponentMetadata mcm = cm.toManageableComponentMetadata();
      pw.printf("%s: {\n", mcm.getJmxObjectName());
      for (JmxAttributeMetadata s : mcm.getAttributeMetadata()) {
         pw.printf("  %s: %s\n", s.getName(), getAttributeValue(component, s));
      }
      pw.println("}");
   }

   private Object getAttributeValue(Object o, JmxAttributeMetadata attr) {
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
