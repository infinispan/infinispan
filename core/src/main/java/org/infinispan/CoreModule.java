package org.infinispan;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.RolePermissionMapper;

/**
 * @api.private
 */
@InfinispanModule(name = "core")
public class CoreModule implements ModuleLifecycle {
   @Override
   public void cacheManagerStarted(GlobalComponentRegistry gcr) {
      gcr.getComponent(GlobalConfigurationManager.class).postStart();
      startLifecycleComponent(gcr, RolePermissionMapper.class, PrincipalRoleMapper.class);
   }

   @Override
   public void cacheManagerStopping(GlobalComponentRegistry gcr) {
      stopLifecycleComponent(gcr, RolePermissionMapper.class, PrincipalRoleMapper.class);
   }

   public static void startLifecycleComponent(GlobalComponentRegistry gcr, Class<?>... klasses) {
      for (Class<?> klass : klasses) {
         if (gcr.getComponent(klass) instanceof Lifecycle l) {
            l.start();
         }
      }
   }

   public static void stopLifecycleComponent(GlobalComponentRegistry gcr, Class<?>... klasses) {
      for (Class<?> klass : klasses) {
         if (gcr.getComponent(klass) instanceof Lifecycle l) {
            l.stop();
         }
      }
   }

}
