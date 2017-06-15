package org.infinispan.query.remote;

import org.infinispan.commons.dataconversion.CompatModeEncoder;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * @since 9.1
 */
public class ProtostreamCompatEncoder extends CompatModeEncoder {

   public ProtostreamCompatEncoder(EmbeddedCacheManager cm) {
      super(new CompatibilityProtoStreamMarshaller());
      ((CompatibilityProtoStreamMarshaller) marshaller).injectDependencies(cm);
   }

}
