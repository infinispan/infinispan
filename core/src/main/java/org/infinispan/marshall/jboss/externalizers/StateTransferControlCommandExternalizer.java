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

import org.infinispan.commands.control.StateTransferControlCommand; 
import org.infinispan.remoting.transport.Transport;
import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

import java.io.IOException;

/**
 * StateTransferControlCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 * @deprecated With new ObjecTable based solution, we're now fully in control of the stream, 
 * so no need to put class on the wire. As a result, we can use the exact same trick used
 * by the old marshaller implementation which uses a RemoteCommandFactory to load the class.
 */
@Deprecated
public class StateTransferControlCommandExternalizer extends ReplicableCommandExternalizer {
   /** The serialVersionUID */
   private static final long serialVersionUID = -3743458410265076691L;
   private Transport transport;

   public void init(Transport transport) {
      this.transport = transport;
   }

//   @Override
//   protected void writeClass(Marshaller output, Class<?> subjectType) throws IOException {
//      // No-op
//   }
//
//   @Override
//   protected Object createExternal(Unmarshaller input) throws IOException, ClassNotFoundException {
//      StateTransferControlCommand command = new StateTransferControlCommand();
//      command.init(transport);
//      return command;
//   }
}