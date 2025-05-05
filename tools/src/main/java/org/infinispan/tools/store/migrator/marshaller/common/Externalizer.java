package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

public interface Externalizer<T> extends Serializable {

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

}
