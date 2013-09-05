package org.infinispan.persistence.keymappers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.infinispan.marshall.core.MarshalledValue;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.Base64;

/**
 * This class is an implementation for {@link TwoWayKey2StringMapper} that supports both primitives
 * and {@link MarshalledValue}s. It extends {@link DefaultTwoWayKey2StringMapper} to achieve this.
 *
 * @author Justin Hayes
 * @since 5.2
 */
public class MarshalledValueOrPrimitiveMapper extends DefaultTwoWayKey2StringMapper implements MarshallingTwoWayKey2StringMapper {

   private MarshalledValue.Externalizer externalizer;

   @Override
   public void setMarshaller(StreamingMarshaller marshaller) {
      externalizer = new MarshalledValue.Externalizer(marshaller);
   }

   @Override
   public String getStringMapping(Object key) {
      if (super.isSupportedType(key.getClass())) {
         // Use our parent
         return super.getStringMapping(key);
      } else {
         // Do it ourself
         try {
            MarshalledValue mv = (MarshalledValue) key;
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
            MarshalledValue mv = (MarshalledValue) obj;
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
   private String serializeObj(MarshalledValue mv) throws Exception {
      if(externalizer==null)
         throw new IllegalStateException("Cannot serialize object: undefined marshaller");
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      externalizer.writeObject(oos, mv);
      oos.close();
      return Base64.encodeBytes(baos.toByteArray());
   }

   /**
    *
    * Use MarshalledValue.Externalizer to deserialize.
    *
    * @param key
    * @return
    * @throws Exception
    */
   private MarshalledValue deserializeObj(String key) throws Exception {
      if(externalizer==null)
         throw new IllegalStateException("Cannot deserialize object: undefined marshaller");
      byte[] data = Base64.decode(key);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
      MarshalledValue mv = externalizer.readObject(ois);
      ois.close();
      return mv;
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return keyType.equals(MarshalledValue.class) || super.isSupportedType(keyType);
   }
}
