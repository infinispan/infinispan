package org.infinispan.rest;

import static org.infinispan.server.core.ExternalizerIds.MIME_METADATA;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.AbstractModuleLifecycle;

/**
 * Module lifecycle callbacks implementation that enables module specific {@link org.infinispan.marshall.AdvancedExternalizer}
 * implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class LifecycleCallbacks extends AbstractModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      globalConfiguration.serialization().advancedExternalizers().put(
            MIME_METADATA, new MimeMetadata.Externalizer());
   }
}
