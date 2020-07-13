package org.infinispan.encoding.impl;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.TranscoderMarshallerAdapter;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;

/**
 * @since 9.2
 */
public class JavaSerializationTranscoder extends TranscoderMarshallerAdapter {

   public JavaSerializationTranscoder(ClassAllowList classAllowList) {
      super(new JavaSerializationMarshaller(classAllowList));
   }

}
