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
package org.infinispan.marshall.jboss.externalizers;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.marshall.jboss.Externalizer;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * MarshalledValueExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class MarshalledValueExternalizer implements Externalizer {
   private org.infinispan.marshall.Marshaller ispnMarshaller;
   
   public void init(org.infinispan.marshall.Marshaller ispnMarshaller) {
      this.ispnMarshaller = ispnMarshaller;
   }
   
   public void writeObject(Marshaller output, Object subject) throws IOException {
      MarshalledValue mv = ((MarshalledValue) subject);
      byte[] raw = mv.getRaw();
      UnsignedNumeric.writeUnsignedInt(output, raw.length);
      output.write(raw);
      output.writeInt(mv.hashCode());      
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      int length = UnsignedNumeric.readUnsignedInt(input);
      byte[] raw = new byte[length];
      input.readFully(raw);
      int hc = input.readInt();
      return new MarshalledValue(raw, hc, ispnMarshaller);
   }

}
