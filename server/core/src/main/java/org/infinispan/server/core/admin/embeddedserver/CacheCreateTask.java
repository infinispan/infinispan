package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to create a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to remove</li>
 *    <li><strong>flags</strong> </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheCreateTask extends AdminServerTask<Void> {

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "create";
   }

   @Override
   public Void call() throws Exception {
      if (isPersistent())
         throw new UnsupportedOperationException();

      String name = requireParameter("name");
      String template = getParameter("template");
      cacheManager.executor().submitConsumer(localManager -> {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         if (template != null) {
            builder.read(localManager.getCacheConfiguration(template));
         } else {
            builder.read(localManager.getDefaultCacheConfiguration());
         }
         builder.template(false);
         localManager.defineConfiguration(name, builder.build());
         localManager.getCache(name);
         return null;
      }, (address, value, throwable) -> {
         if (throwable != null) {
            log.fatal("Cache startup encountered exception on node " + address, throwable);
         }
      }).join();

      return null;
   }
}
