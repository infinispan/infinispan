/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
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

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.io.ByteBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * A marshaller is a class that is able to marshall and unmarshall objects efficiently.
 * <p/>
 * The reason why this is implemented specially in Infinispan rather than resorting to Java serialization or even the
 * more efficient JBoss serialization is that a lot of efficiency can be gained when a majority of the serialization
 * that occurs has to do with a small set of known types such as {@link org.infinispan.transaction.GlobalTransaction} or
 * {@link org.infinispan.commands.ReplicableCommand}, and class type information can be replaced with simple magic
 * numbers.
 * <p/>
 * Unknown types (typically user data) falls back to JBoss serialization.
 * <p/>
 * In addition, using a marshaller allows adding additional data to the byte stream, such as context class loader
 * information on which class loader to use to deserialize the object stream, or versioning information to allow streams
 * to interoperate between different versions of Infinispan (see {@link VersionAwareMarshaller}
 * <p/>
 * This interface is used to marshall {@link org.infinispan.commands.ReplicableCommand}s, their parameters and their
 * response values.
 * <p/>
 * The interface is also used by the {@link org.infinispan.loaders.CacheStore} framework to efficiently serialize data
 * to be persisted, as well as the {@link org.infinispan.statetransfer.StateTransferManager} when serializing the cache
 * for transferring state en-masse.
 *
 * @author <a href="mailto://manik@jboss.org">Manik Surtani</a>
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface Marshaller {

   /**
    * Create and open a new ObjectOutput for the given output stream. This method should be used for opening data outpus
    * when multiple objectToObjectStream() calls will be made before the stream is closed
    *
    * @param os output stream
    * @return ObjectOutput to write to
    * @throws IOException
    */
   ObjectOutput startObjectOutput(OutputStream os) throws IOException;

   /**
    * Finish using the given ObjectOutput. After opening a ObjectOutput and calling objectToObjectStream() mutliple
    * times, use this method to flush the data and close if necessary
    *
    * @param oo data output that finished using
    */
   void finishObjectOutput(ObjectOutput oo);

   /**
    * Marshalls an object to a given {@link java.io.ObjectOutput}
    *
    * @param obj object to marshall
    * @param out stream to marshall to
    */
   void objectToObjectStream(Object obj, ObjectOutput out) throws IOException;

   /**
    * Create and open a new ObjectInput for the given input stream. This method should be used for opening data inputs
    * when multiple objectFromObjectStream() calls will be made before the stream is closed.
    *
    * @param is input stream
    * @return ObjectInput to read from
    * @throws IOException
    */
   ObjectInput startObjectInput(InputStream is) throws IOException;

   /**
    * Finish using the given ObjectInput. After opening a ObjectInput and calling objectFromObjectStream() mutliple
    * times, use this method to flush the data and close if necessary
    *
    * @param oi data input that finished using
    */
   void finishObjectInput(ObjectInput oi);

   /**
    * Unmarshalls an object from an {@link java.io.ObjectInput}
    *
    * @param in stream to unmarshall from
    */
   Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException;

   /**
    * A method that returns an instance of {@link org.infinispan.io.ByteBuffer}, which allows direct access to the byte
    * array with minimal array copying
    *
    * @param o object to marshall
    * @return a ByteBuffer
    * @throws Exception
    */
   ByteBuffer objectToBuffer(Object o) throws IOException;

   Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException;

   byte[] objectToByteBuffer(Object obj) throws IOException;

   Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException;

   Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException;
}