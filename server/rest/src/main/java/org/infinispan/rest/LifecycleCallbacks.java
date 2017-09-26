package org.infinispan.rest;

import static org.infinispan.server.core.ExternalizerIds.MIME_METADATA;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.rest.operations.mime.MimeMetadata;

/**
 * Module lifecycle callbacks implementation that enables module specific {@link org.infinispan.commons.marshall.AdvancedExternalizer}
 * implementations to be registered.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class LifecycleCallbacks implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      globalConfiguration.serialization().advancedExternalizers().put(
            MIME_METADATA, new MimeMetadata.Externalizer());
   }
}
