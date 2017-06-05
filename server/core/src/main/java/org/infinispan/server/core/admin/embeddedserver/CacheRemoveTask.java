package org.infinispan.server.core.admin.embeddedserver;

import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to remove a cache
 * Parameters:
 * <ul>
 *    <li><strong>name</strong> the name of the cache to remove</li>
 *    <li><strong>flags</strong> </li>
 * </ul>
 *
 * @author Tristan Tarrant
 * @since 9.1
 */
public class CacheRemoveTask extends AdminServerTask<Void> {

   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "remove";
   }

   @Override
   public Void call() throws Exception {
      if (isPersistent())
         throw new UnsupportedOperationException();

      String name = requireParameter("name");
      cacheManager.removeCache(name);
      return null;
   }
}
