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

import org.infinispan.marshall.jboss.Externalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * GlobalTransactionExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
@Immutable
public class GlobalTransactionExternalizer implements Externalizer {
   /** The serialVersionUID */
   private static final long serialVersionUID = -8677909497367726531L;

   public void writeObject(Marshaller output, Object subject) throws IOException {
      GlobalTransaction gtx = (GlobalTransaction) subject;
      output.writeLong(gtx.getId());
      output.writeObject(gtx.getAddress());      
   }

   public Object readObject(Unmarshaller input) throws IOException, ClassNotFoundException {
      GlobalTransaction gtx = new GlobalTransaction();
      long id = input.readLong();
      Object address = input.readObject();
      gtx.setId(id);
      gtx.setAddress((Address) address);
      return gtx;
   }

}
