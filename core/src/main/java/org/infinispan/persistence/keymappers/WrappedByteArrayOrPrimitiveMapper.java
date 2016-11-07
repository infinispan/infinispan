package org.infinispan.persistence.keymappers;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.Base64;

/**
 * This class is an implementation for {@link TwoWayKey2StringMapper} that supports both primitives
 * and {@link org.infinispan.commons.marshall.WrappedByteArray}s. It extends {@link DefaultTwoWayKey2StringMapper}
 * to achieve this.
 *
 * @author Justin Hayes
 * @since 5.2
 */
public class WrappedByteArrayOrPrimitiveMapper extends DefaultTwoWayKey2StringMapper implements MarshallingTwoWayKey2StringMapper {

   private StreamingMarshaller externalizer;

   @Override
   public void setMarshaller(StreamingMarshaller marshaller) {
      externalizer = marshaller;
   }

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

   /**
    * Use MarshalledValue.Externalizer to serialize.
    *
    * @param mv
    * @return
    * @throws Exception
    */
   private String serializeObj(WrappedByteArray mv) throws Exception {
//      if(externalizer==null)
//         throw new IllegalStateException("Cannot serialize object: undefined marshaller");
//      ByteArrayOutputStream baos = new ByteArrayOutputStream();
//      ObjectOutputStream oos = new ObjectOutputStream(baos);
//      externalizer.writeObject(oos, mv.getBytes());
//      oos.close();
      return Base64.encodeBytes(mv.getBytes());
   }

   /**
    *
    * Use MarshalledValue.Externalizer to deserialize.
    *
    * @param key
    * @return
    * @throws Exception
    */
   private WrappedByteArray deserializeObj(String key) throws Exception {
      if(externalizer==null)
         throw new IllegalStateException("Cannot deserialize object: undefined marshaller");
      byte[] data = Base64.decode(key);
//      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
//      MarshalledValue mv = externalizer.readObject(ois);
//      ois.close();
//      return mv;
      return new WrappedByteArray(data);
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return keyType.equals(WrappedByteArray.class) || super.isSupportedType(keyType);
   }
}
