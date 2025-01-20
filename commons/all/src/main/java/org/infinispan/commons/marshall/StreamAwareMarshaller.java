package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;

import net.jcip.annotations.ThreadSafe;

/**
 * A minimal interface that facilitates the marshalling/unmarshalling of objects from the provided {@link
 * java.io.InputStream}/{@link java.io.OutputStream}.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ThreadSafe
public interface StreamAwareMarshaller {

   /**
    * Marshall an object to the {@link OutputStream}
    *
    * @param o   the object to write to the {@link OutputStream}
    * @param out the {@link OutputStream} to write the object to
    * @throws IOException if the object cannot be marshalled to the {@link OutputStream} due to some I/O error
    */
   void writeObject(Object o, OutputStream out) throws IOException;

   /**
    * Unmarshall an object from the {@link InputStream}
    *
    * @param in the {@link InputStream} to unmarshall an object from
    * @return the unmarshalled object instance
    * @throws IOException            if unmarshalling cannot complete due to some I/O error
    * @throws ClassNotFoundException if the class of the object trying to unmarshall is not found
    */
   Object readObject(InputStream in) throws ClassNotFoundException, IOException;

   /**
    * A method that checks whether the given object is marshallable as per the rules of this marshaller.
    *
    * @param o object to verify whether it's marshallable or not
    * @return true if the object is marshallable, otherwise false
    */
   boolean isMarshallable(Object o);

   /**
    * An method that provides an estimate of the buffer size that will be required once the object has been marshalled.
    *
    * @param o instance that will be stored in the buffer.
    * @return int representing the next predicted buffer size.
    */
   int sizeEstimate(Object o);

   /**
    * @return the {@link MediaType} associated with the content produced by the marshaller
    */
   MediaType mediaType();
}
