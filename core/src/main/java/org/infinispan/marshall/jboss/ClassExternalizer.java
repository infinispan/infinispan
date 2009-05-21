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

import org.jboss.marshalling.Marshaller;
import org.jboss.marshalling.Unmarshaller;

/**
 * JBoss Marshalling implementation is now based only on ObjectTable instances
 * and so JBMAR ClassTable is not used any longer. Instead, for those cases where
 * classes need to be written, a new ClassTable has been redefined that allows 
 * reducing the number of writes written for a Class.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface ClassExternalizer {
   /**
    * Write the predefined class reference to the stream.
    *
    * @param output the marshaller to write to
    * @param subjectType the class reference to write
    * @throws IOException if an I/O error occurs
    */
   void writeClass(Marshaller output, Class<?> subjectType) throws IOException;
   
   /**
    * Read a class from the stream.  The class will have been written by the
    * writeClass(Marshaller, Class) method, as defined above.
    *
    * @param unmarshaller the unmarshaller to read from
    * @return the class
    * @throws IOException if an I/O error occurs
    */
   Class<?> readClass(Unmarshaller input) throws IOException;
      
   /**
    * Classes that provide a shortened version of Class information should
    * implement this interface in order to get a callback to set the ClassTable. 
    */
   interface ClassWritable {
      /**
       * Set ClassTable callback to be able to provide a write a shortened 
       * Class version.
       * 
       * @param classTable
       */
      void setClassExternalizer(ClassExternalizer classExt);
   }
}
