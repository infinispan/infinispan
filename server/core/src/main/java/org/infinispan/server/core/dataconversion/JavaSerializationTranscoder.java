package org.infinispan.server.core.dataconversion;

import org.infinispan.commons.configuration.ClassWhiteList;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * @since 9.2
 */
public class JavaSerializationTranscoder extends TranscoderMarshallerAdapter {

   public JavaSerializationTranscoder(ClassWhiteList classWhiteList) {
      super(new JavaSerializationMarshaller(classWhiteList));
   }

}
