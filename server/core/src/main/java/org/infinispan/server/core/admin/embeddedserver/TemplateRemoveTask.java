package org.infinispan.server.core.admin.embeddedserver;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.admin.AdminServerTask;

/**
 *  * Admin operation to remove a template
 *  * Parameters:
 *  * <ul>
 *  *    <li><strong>name</strong> the name of the template to remove</li>
 *  *    <li><strong>flags</strong> any flags, e.g. PERMANENT</li>
 *  * </ul>
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public class TemplateRemoveTask extends AdminServerTask<Void> {
   private static final Set<String> PARAMETERS;

   static {
      Set<String> params = new HashSet<>(1);
      params.add("name");
      PARAMETERS = Collections.unmodifiableSet(params);
   }

   @Override
   public String getTaskContextName() {
      return "template";
   }

   @Override
   public String getTaskOperationName() {
      return "remove";
   }

   @Override
   public Set<String> getParameters() {
      return PARAMETERS;
   }

   @Override
   protected Void execute(EmbeddedCacheManager cacheManager, Map<String, List<String>> parameters,
                          EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      String name = requireParameter(parameters,"name");
      cacheManager.administration().withFlags(flags).removeTemplate(name);
      return null;
   }
}
