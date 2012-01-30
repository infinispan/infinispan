/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
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

import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * A specialization of {@link Marshaller} that supports streams.
 * <p/>
 * A single instance of any implementation is shared by multiple threads, so implementations <i>need</i> to be threadsafe,
 * and preferably immutable.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 *
 * @see Marshaller
 */
@ThreadSafe
public interface StreamingMarshaller extends Marshaller {
   
   void stop();

   /**
    * <p>Create and open an ObjectOutput instance for the given output stream. This method should be used for opening data 
    * outputs when multiple objectToObjectStream() calls will be made before the stream is closed by calling finishObjectOutput().</p>
    * 
    * <p>This method also takes a boolean that represents whether this particular call to startObjectOutput() is reentrant 
    * or not. A call to startObjectOutput() should be marked reentrant whenever a 2nd or more calls to this method are made 
    * without having called finishObjectOutput() first.     
    * 
    * <p>To potentially speed up calling startObjectOutput multiple times in a non-reentrant way, i.e. 
    * startObjectOutput/finishObjectOutput...startObjectOutput/finishObjectOutput...etc, which is is the most common case, the 
    * StreamingMarshaller implementation could potentially use some mechanisms to speed up this startObjectOutput call.
    *  
    * <p>On the other hand, when a call is reentrant, i.e. startObjectOutput/startObjectOutput(reentrant)...finishObjectOutput/finishObjectOutput, 
    * the StreamingMarshaller implementation might treat it differently. An example of reentrancy would be marshalling of {@link MarshalledValue}.
    * When sending or storing a MarshalledValue, a call to startObjectOutput() would occur so that the stream is open and 
    * following, a 2nd call could occur so that MarshalledValue's raw byte array version is calculated and sent across. 
    * This enables storing as binary on the receiver side which is performance gain. The StreamingMarshaller implementation could decide
    * that it needs a separate ObjectOutput or similar for the 2nd call since it's aim is only to get the raw byte array version 
    * and the close finish with it.</p>
    *
    * @param os output stream
    * @param isReentrant whether the call is reentrant or not.
    * @param estimatedSize estimated size in bytes of the output. Only meant as a possible performance optimization.
    * @return ObjectOutput to write to
    * @throws IOException
    */
   ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, final int estimatedSize) throws IOException;

   /**
    * Finish using the given ObjectOutput. After opening a ObjectOutput and calling objectToObjectStream() multiple
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
    * <p>Create and open a new ObjectInput for the given input stream. This method should be used for opening data inputs
    * when multiple objectFromObjectStream() calls will be made before the stream is closed.</p>
    * 
    * <p>This method also takes a boolean that represents whether this particular call to startObjectInput() is reentrant 
    * or not. A call to startObjectInput() should be marked reentrant whenever a 2nd or more calls to this method are made 
    * without having called finishObjectInput() first.</p>     
    * 
    * <p>To potentially speed up calling startObjectInput multiple times in a non-reentrant way, i.e. 
    * startObjectInput/finishObjectInput...startObjectInput/finishObjectInput...etc, which is is the most common case, the 
    * StreamingMarshaller implementation could potentially use some mechanisms to speed up this startObjectInput call.</p> 
    *  
    * @param is input stream
    * @param isReentrant whether the call is reentrant or not.
    * @return ObjectInput to read from
    * @throws IOException
    */
   ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException;

   /**
    * Finish using the given ObjectInput. After opening a ObjectInput and calling objectFromObjectStream() multiple
    * times, use this method to flush the data and close if necessary
    *
    * @param oi data input that finished using
    */
   void finishObjectInput(ObjectInput oi);

   /**
    * Unmarshalls an object from an {@link java.io.ObjectInput}
    *
    * @param in stream to unmarshall from
    * @throws IOException if unmarshalling cannot complete due to some I/O error
    * @throws ClassNotFoundException if the class of the object trying to unmarshall is unknown
    * @throws InterruptedException if the unmarshalling was interrupted. Clients should take this as a sign that
    * the marshaller is no longer available, maybe due to shutdown, and so no more unmarshalling should be attempted.
    */
   Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException;

   Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException;
}
