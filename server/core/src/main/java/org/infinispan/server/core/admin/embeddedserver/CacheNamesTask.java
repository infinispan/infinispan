package org.infinispan.server.core.admin.embeddedserver;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 * Admin operation to obtain a list of caches
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
public class CacheNamesTask extends AdminServerTask<byte[]> {
   @Override
   public String getTaskContextName() {
      return "cache";
   }

   @Override
   public String getTaskOperationName() {
      return "names";
   }

   @Override
   public Set<String> getParameters() {
      return Collections.emptySet();
   }

   @Override
   protected byte[] execute(EmbeddedCacheManager cacheManager, Map<String, String> parameters, EnumSet<CacheContainerAdmin.AdminFlag> adminFlags) {
      Set<String> cacheNames = cacheManager.getCacheNames();
      StringBuilder sb = new StringBuilder("[");
      for(String s : cacheNames) {
         if (sb.length() > 1)
            sb.append(',');
         sb.append('"');
         sb.append(s);
         sb.append('"');
      }
      sb.append(']');
      return sb.toString().getBytes(StandardCharsets.UTF_8);
   }
}
