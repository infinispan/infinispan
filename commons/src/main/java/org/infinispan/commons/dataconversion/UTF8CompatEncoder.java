package org.infinispan.commons.dataconversion;

import org.infinispan.commons.marshall.UTF8StringMarshaller;

/**
 * @since 9.2
 */
public class UTF8CompatEncoder extends CompatModeEncoder {

   public static final UTF8CompatEncoder INSTANCE = new UTF8CompatEncoder();

   public UTF8CompatEncoder() {
      super(new UTF8StringMarshaller());
   }

   @Override
   public short id() {
      return EncoderIds.UTF8_COMPAT;
   }
}
