package org.infinispan.tree;

import org.infinispan.config.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

/**
 * Module lifecycle callbacks implementation that enables module specific
 * {@link org.infinispan.marshall.Externalizer} implementations to be registered.
 *
 * Information about the valid id range can be found <a href="http://community.jboss.org/docs/DOC-16198">here</a>
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr) {
      GlobalConfiguration globalCfg = gcr.getGlobalConfiguration();
      globalCfg.addExternalizer(1000, new NodeKey.Externalizer());
      globalCfg.addExternalizer(1001, new Fqn.Externalizer());
   }

}
