package org.infinispan.transcoder.xml;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * XML Transcoder module lifecycle callbacks.
 *
 * <p>Registers a transcoder for converting to/from XML using XStream.</p>
 *
 * @since 16.3
 */
@InfinispanModule(name = "xml-transcoder", requiredModules = "core")
public class XmlTranscoderModule implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      ClassAllowList classAllowList = gcr.getComponent(EmbeddedCacheManager.class).getClassAllowList();
      ClassLoader classLoader = globalConfiguration.classLoader();

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      encoderRegistry.registerTranscoder(new XMLTranscoder(classLoader, classAllowList));
   }
}
