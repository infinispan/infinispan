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
package org.infinispan.marshall.jboss;

import java.io.IOException;

import org.jboss.marshalling.Unmarshaller;
import org.jboss.marshalling.ObjectTable.Writer;

/**
 * Extended interface that extends capabilities of writing predefined objects 
 * with the possibility of reading them. Any new externalizer implementations
 * should implement this interface. Implementations are free to write class 
 * information to the wire as they wish. See {@link ReplicableCommandExternalizer} 
 * for an such example.
 * 
 * To add a new non-user Externalizer, follow these steps:
 * 
 * 1. Create an implementation of Externalizer.
 * 
 * 2. Add Class to Externalizer mapping to ConstantObjectTable.EXTERNALIZERS
 * 
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Externalizer extends Writer {
   
   /**
    * Read an instance from the stream.  The instance will have been written by the
    * {@link #writeObject(Object)} method.
    *
    * @param unmarshaller the unmarshaller to read from
    * @return the object instance
    * @throws IOException if an I/O error occurs
    * @throws ClassNotFoundException if a class could not be found
    */
   Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException;
}
