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
package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * One of the key aspects of Infinispan is that it often needs to marshall/unmarshall
 * objects in order to provide some of its functionality.  For example, if it needs
 * to store objects in a write-through or write-behind cache store, the objects stored
 * need marshalling.  If a cluster of Infinispan nodes is formed, objects shipped around
 * need marshalling.  Even if you enable lazy deserialization, objects need to marshalled
 * so that they can be lazily unmarshalled with the correct classloader.
 *
 * Using standard JDK serialization is slow and produces payloads that are too big and
 * can affect bandwidth usage.  On top of that, JDK serialization does not work well with
 * objects that are supposed to be immutable.  In order to avoid these issues, Infinispan
 * uses JBoss Marshalling for marshalling/unmarshalling objects.  JBoss Marshalling is fast
 * , provides very space efficient payloads, and on top of that, allows users to construct
 * objects themselves during unmarshalling, hence allowing objects to carry on being immutable.
 *
 * Starting with 5.0, users of Infinispan can now benefit from this marshalling
 * framework as well, and they can provide their own implementations of the Externalizer<T>
 * interface in order to define, how a particular object type needs to be marshalled or
 * unmarshalled.
 *
 * It's common practice to include Externalizer implementations within the classes that
 * they marshall/unmarshall as public static classes.  To make Externalizer implementations
 * easier to code and more typesafe, make sure you define type <T> as the type of object
 * that's being marshalled/unmarshalled.  You can find plenty of examples of Externalizer
 * implementations in the Infinispan code base, but to highlight one, check the Externalizer
 * implementation for {@link org.infinispan.remoting.transport.jgroups.JGroupsAddress} in
 * {@link org.infinispan.remoting.transport.jgroups.JGroupsAddress.Externalizer}
 *
 * {@link AbstractExternalizer} provides default implementations for some of the methods
 * defined in this interface and so it's generally recommended that implementations extend
 * that abstract class instead of implementing {@link Externalizer} directly.
 *
 * @author Galder Zamarreño
 * @since 4.0
 */
public interface Externalizer<T> {
   
   /**
    * Write the object reference to the stream.
    *
    * @param output the object output to write to
    * @param object the object reference to write
    * @throws IOException if an I/O error occurs
    */
   void writeObject(ObjectOutput output, T object) throws IOException;
   
   /**
    * Read an instance from the stream.  The instance will have been written by the
    * {@link #writeObject(ObjectOutput, Object)} method.  Implementations are free
    * to create instances of the object read from the stream in any way that they
    * feel like. This could be via constructor, factory or reflection.
    *
    * @param input the object input to read from
    * @return the object instance
    * @throws IOException if an I/O error occurs
    * @throws ClassNotFoundException if a class could not be found
    */
   T readObject(ObjectInput input) throws IOException, ClassNotFoundException;

   /**
    * Returns a collection of Class instances representing the types that this
    * Externalizer can marshall. Clearly, empty sets are not allowed.
    *
    * @return A set containing the Class instances that can be marshalled.
    */
   Set<Class<? extends T>> getTypeClasses();

   /**
    * Returns an integer that identifies the externalizer type. This is used
    * at read time to figure out which {@link Externalizer} should read the
    * contents of the incoming buffer.
    *
    * Using a positive integer allows for very efficient variable length
    * encoding of numbers, and it's much more efficient than shipping
    * {@link Externalizer} implementation class information around. Negative
    * values are not allowed.
    *
    * Implementers of this interface can use any positive integer as long as
    * it does not clash with any other identifier in the system. You can find
    * information on the pre-assigned identifier ranges in
    * <a href="http://community.jboss.org/docs/DOC-16198">here</a>.
    *
    * It's highly recommended that maintaining of these identifiers is done
    * in a centralized way and you can do so by making annotations reference
    * a set of statically defined identifiers in a separate class or
    * interface. Such class/interface gives a global view of the identifiers
    * in use and so can make it easier to assign new ids.
    *
    * Implementors can optionally avoid giving a meaningful implementation to
    * this method (i.e. return null) and instead rely on XML or programmatic
    * configuration to provide the Externalizer id. If no id can be determined
    * via the implementation or XML/programmatic configuration, an error will
    * be reported. If an id has been defined both via the implementation and
    * XML/programmatic configuration, the value defined via XML/programmatic
    * configuration will be used ignoring the other.
    *
    * @return A positive identifier for the Externalizer.
    */
   Integer getId();

}
