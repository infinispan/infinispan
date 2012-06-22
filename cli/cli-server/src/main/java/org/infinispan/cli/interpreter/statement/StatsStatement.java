/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.statement;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.List;

import org.infinispan.Cache;
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

/**
 * Displays statistics about a container or a cache
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class StatsStatement implements Statement {
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
            if ("container".equals(option.getName())) {
               printContainerStats(pw, (DefaultCacheManager) session.getCacheManager());
               pw.flush();
            } else {
               throw new StatementException("Unkown option: " + option);
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
      pw.printf("  createdCacheCount: %d\n", cacheManager.getCreatedCacheCount());
      pw.printf("  definedCacheCount: %d\n", cacheManager.getDefinedCacheCount());
      pw.printf("  definedCacheNames: %s\n", cacheManager.getDefinedCacheNames());
      pw.printf("  version: %s\n", cacheManager.getVersion());
      pw.println("}");
   }

   private void printCacheStats(PrintWriter pw, Cache<?, ?> cache) throws StatementException {
      if (!cache.getCacheConfiguration().jmxStatistics().enabled()) {
         throw new StatementException("Statistics are not enabled for the specified cache");
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
