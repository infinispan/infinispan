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
import org.jboss.marshalling.Creator;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * StateTransferControlCommandExternalizer.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public class StateTransferControlCommandExternalizer extends ReplicableCommandExternalizer {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -3743458410265076691L;

   private Transport transport;

   public void init(Transport transport) {
      this.transport = transport;
   }

   /**
    * In this case, subjectType will contain the class name of the ReplicableCommand subclass to create.
    * <p/>
    * Note that StateTransferControlCommand might need to be treated differently!!! Todo: check outcome of email sent to
    * dev list.
    */
   public Object createExternal(Class<?> subjectType, ObjectInput input, Creator defaultCreator)
         throws IOException, ClassNotFoundException {
      StateTransferControlCommand command = new StateTransferControlCommand();
      command.init(transport);
      return command;
   }
}