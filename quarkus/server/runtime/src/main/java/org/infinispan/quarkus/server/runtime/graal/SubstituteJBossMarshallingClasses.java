package org.infinispan.quarkus.server.runtime.graal;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.LifecycleCallbacks;
import org.infinispan.persistence.remote.upgrade.MigrationTask;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;

// We need to remove all the MigrationTask and related classes as they load up JBoss Marshalling
public class SubstituteJBossMarshallingClasses {
}

@TargetClass(MigrationTask.class)
final class Target_MigrationTask {
   @Substitute
   public Integer apply(EmbeddedCacheManager embeddedCacheManager) {
      throw org.infinispan.quarkus.embedded.runtime.Util.unsupportedOperationException("Migration Task");
   }
}

@TargetClass(LifecycleCallbacks.class)
final class Target_org_infinispan_persistence_remote_LifecycleCallbacks {
   @Substitute
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalCfg) { }
}
