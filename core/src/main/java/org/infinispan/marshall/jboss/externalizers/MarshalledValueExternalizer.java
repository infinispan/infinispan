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

import net.jcip.annotations.Immutable;
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
@Immutable
public class MarshalledValueExternalizer implements Externalizer {
   /** The serialVersionUID */
   private static final long serialVersionUID = 8473423584918714661L;

   public void writeObject(Marshaller output, Object subject) throws IOException {
      MarshalledValue mv = ((MarshalledValue) subject);
      byte[] raw = mv.getRaw();
      UnsignedNumeric.writeUnsignedInt(output, raw.length);
      output.write(raw);
      output.writeInt(mv.hashCode());      
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      MarshalledValue mv = new MarshalledValue();
      int length = UnsignedNumeric.readUnsignedInt(input);
      byte[] b = new byte[length];
      input.readFully(b);
      int hc = input.readInt();
      mv.init(b, hc);
      return mv;
   }

}
