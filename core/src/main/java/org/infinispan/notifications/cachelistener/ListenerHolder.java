package org.infinispan.notifications.cachelistener;

import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;

/**
 * @since 9.1
 */
public class ListenerHolder {

   private final Object listener;
   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;

   public ListenerHolder(Object listener, Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass,
                         Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      this.listener = listener;
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
   }

   public Object getListener() {
      return listener;
   }

   public Class<? extends Encoder> getValueEncoderClass() {
      return valueEncoderClass;
   }

   public Class<? extends Wrapper> getKeyWrapperClass() {
      return keyWrapperClass;
   }

   public Class<? extends Wrapper> getValueWrapperClass() {
      return valueWrapperClass;
   }

   public Class<? extends Encoder> getKeyEncoderClass() {
      return keyEncoderClass;
   }

}
