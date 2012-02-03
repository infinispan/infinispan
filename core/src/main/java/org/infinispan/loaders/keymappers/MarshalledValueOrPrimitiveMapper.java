/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.keymappers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.infinispan.loaders.keymappers.DefaultTwoWayKey2StringMapper;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.util.Base64;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class is an implementation for {@link TwoWayKey2StringMapper} that supports both primitives
 * and {@link MarshalledValue}s. It extends {@link DefaultTwoWayKey2StringMapper} to achieve this.
 * 
 * @author Justin Hayes
 * @since 5.1
 */
public class MarshalledValueOrPrimitiveMapper extends DefaultTwoWayKey2StringMapper {
   private static final Log log = LogFactory.getLog(MarshalledValueOrPrimitiveMapper.class);

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
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      MarshalledValue.Externalizer externalizer = new MarshalledValue.Externalizer();
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
      byte[] data = Base64.decode(key);
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
      MarshalledValue.Externalizer externalizer = new MarshalledValue.Externalizer();
      MarshalledValue mv = externalizer.readObject(ois);
      ois.close();
      return mv;
   }

   @Override
   public boolean isSupportedType(Class<?> keyType) {
      return keyType.equals(MarshalledValue.class) || super.isSupportedType(keyType);
   }
}
