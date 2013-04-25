/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.cli.interpreter.codec;

import org.infinispan.cli.interpreter.logging.Log;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodCodec.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class HotRodCodec implements Codec {
   public static final Log log = LogFactory.getLog(HotRodCodec.class, Log.class);
   Marshaller marshaller = new GenericJBossMarshaller(); // FIXME: assumes that clients will marshall using this

   @Override
   public String getName() {
      return "hotrod";
   }

   @Override
   public Object encodeKey(Object key) throws CodecException {
      if (key != null) {
         try {
            return marshaller.objectToByteBuffer(key);
         } catch (Exception e) {
            throw log.keyEncodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object encodeValue(Object value) throws CodecException {
      if (value != null) {
         try {
            return marshaller.objectToByteBuffer(value);
         } catch (Exception e) {
            throw log.valueEncodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeKey(Object key) throws CodecException {
      if (key != null) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) key);
         } catch (Exception e) {
            throw log.keyDecodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

   @Override
   public Object decodeValue(Object value) throws CodecException {
      if (value != null) {
         try {
            return marshaller.objectFromByteBuffer((byte[]) value);
         } catch (Exception e) {
            throw log.valueDecodingFailed(e, this.getName());
         }
      } else {
         return null;
      }
   }

}
