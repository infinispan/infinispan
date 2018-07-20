package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import net.jcip.annotations.ThreadSafe;

/**
 * An extension of the {@link Marshaller} interface that facilitates the marshalling/unmarshalling of objects from
 * the provided {@link java.io.OutputStream}/{@link java.io.InputStream}
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ThreadSafe
public interface StreamAwareMarshaller extends Marshaller {

   /**
    * Marshall an object to the {@link OutputStream}
    *
    * @param o the object to write to the {@link OutputStream}
    * @param out the {@link OutputStream} to write the object to
    * @throws IOException if the object cannot be marshalled to the {@link OutputStream} due to some I/O error
    */
   void writeObject(Object o, OutputStream out) throws IOException;

   /**
    * Unmarshall an object from the {@link InputStream}
    *
    * @param in the {@link InputStream} to unmarshall an object from
    * @return the unmarshalled object instance
    * @throws IOException if unmarshalling cannot complete due to some I/O error
    * @throws ClassNotFoundException if the class of the object trying to unmarshall is not found
    */
   Object readObject(InputStream in) throws ClassNotFoundException, IOException;
}
