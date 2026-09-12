package org.infinispan.transcoder.json;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Transcoder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.encoding.impl.TwoStepTranscoder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.InfinispanModule;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.EncoderRegistry;

/**
 * JSON POJO Transcoder module lifecycle callbacks.
 *
 * <p>Registers a transcoder for converting Java objects to/from JSON using Jackson databind.</p>
 *
 * @since 16.3
 */
@InfinispanModule(name = "json-pojo-transcoder", requiredModules = "core", optionalModules = "jboss-marshalling")
public class JsonPojoModule implements ModuleLifecycle {

   @Override
   public void cacheManagerStarting(GlobalComponentRegistry gcr, GlobalConfiguration globalConfiguration) {
      ClassAllowList classAllowList = gcr.getComponent(EmbeddedCacheManager.class).getClassAllowList();
      ClassLoader classLoader = globalConfiguration.classLoader();

      EncoderRegistry encoderRegistry = gcr.getComponent(EncoderRegistry.class);
      JsonPojoTranscoder jsonPojoTranscoder = new JsonPojoTranscoder(classLoader, classAllowList);
      encoderRegistry.registerTranscoder(jsonPojoTranscoder);

      if (encoderRegistry.isConversionSupported(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING)) {
         Transcoder jbossMarshallingTranscoder = encoderRegistry.getTranscoder(MediaType.APPLICATION_OBJECT, MediaType.APPLICATION_JBOSS_MARSHALLING);
         encoderRegistry.registerTranscoder(new TwoStepTranscoder(jbossMarshallingTranscoder, jsonPojoTranscoder));
      }
   }
}
