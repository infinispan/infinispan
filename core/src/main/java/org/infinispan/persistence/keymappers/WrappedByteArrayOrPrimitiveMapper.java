package org.infinispan.persistence.keymappers;

import java.util.Base64;

import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * This class is an implementation for {@link TwoWayKey2StringMapper} that supports both primitives
 * and {@link org.infinispan.commons.marshall.WrappedByteArray}s. It extends {@link DefaultTwoWayKey2StringMapper}
 * to achieve this.
 *
 * @author Justin Hayes
 * @since 5.2
 */
public class WrappedByteArrayOrPrimitiveMapper extends DefaultTwoWayKey2StringMapper implements MarshallingTwoWayKey2StringMapper {

   @Override
   public String getStringMapping(Object key) {
      if (super.isSupportedType(key.getClass())) {
         // Use our parent
         return super.getStringMapping(key);
      } else {
         // Do it ourself
         try {
            WrappedByteArray mv = (WrappedByteArray) key;
            String serializedObj = serializeObj(mv);
            return serializedObj;
         } catch (Exception ex) {
            throw new IllegalArgumentException("Exception occurred serializing key.", ex);
         }
      }
   }

   @Override
   public Object getKeyMapping(String key) {
      if (super.isSupportedType(key.getClass())) {
         // Use our parent
         return super.getKeyMapping(key);
      } else {
         // Do it ourself
         try {
            Object obj = deserializeObj(key);
            WrappedByteArray mv = (WrappedByteArray) obj;
            return mv;
         } catch (Exception ex) {
            throw new IllegalArgumentException("Exception occurred deserializing key.", ex);
         }
      }
   }

   private String serializeObj(WrappedByteArray mv) {
      return Base64.getEncoder().encodeToString(mv.getBytes());
   }

   private WrappedByteArray deserializeObj(String key) {
      byte[] data = Base64.getDecoder().decode(key);
      return new WrappedByteArray(data);
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return keyType == WrappedByteArray.class || super.isSupportedType(keyType);
   }
}
