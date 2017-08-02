package org.infinispan.notifications.cachelistener;

import org.infinispan.cache.impl.EncodingClasses;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;

/**
 * @since 9.1
 */
public class ListenerHolder {

   private final Object listener;
   private final EncodingClasses encodingClasses;

   public ListenerHolder(Object listener, EncodingClasses encodingClasses) {
      this.listener = listener;
      this.encodingClasses = encodingClasses;
   }

   public Object getListener() {
      return listener;
   }

   public Class<? extends Encoder> getValueEncoderClass() {
      return encodingClasses.getValueEncoderClass();
   }

   public Class<? extends Wrapper> getKeyWrapperClass() {
      return encodingClasses.getKeyWrapperClass();
   }

   public Class<? extends Wrapper> getValueWrapperClass() {
      return encodingClasses.getValueWrapperClass();
   }

   public Class<? extends Encoder> getKeyEncoderClass() {
      return encodingClasses.getKeyEncoderClass();
   }

}
