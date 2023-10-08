package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * One of the key aspects of Infinispan is that it often needs to marshall or
 * unmarshall objects in order to provide some of its functionality.  For
 * example, if it needs to store objects in a write-through or write-behind
 * cache store, the objects stored need marshalling.  If a cluster of
 * Infinispan nodes is formed, objects shipped around need marshalling.  Even
 * if you enable storing as binary, objects need to marshalled so that they
 * can be lazily unmarshalled with the correct classloader.
 *
 * Using standard JDK serialization is slow and produces payloads that are too
 * big and can affect bandwidth usage.  On top of that, JDK serialization does
 * not work well with objects that are supposed to be immutable.  In order to
 * avoid these issues, Infinispan uses JBoss Marshalling for
 * marshalling/unmarshalling objects.  JBoss Marshalling is fast, provides
 * very space efficient payloads, and on top of that, allows users to
 * construct objects themselves during unmarshalling, hence allowing objects
 * to carry on being immutable.
 *
 * Starting with 5.0, users of Infinispan can now benefit from this marshalling
 * framework as well.  In the simplest possible form, users just need to
 * provide an {@link Externalizer} implementation for the type that they want
 * to marshall/unmarshall, and then annotate the marshalled type class with
 * {@link SerializeWith} indicating the externalizer class to use and that's
 * all about it.  At runtime JBoss Marshaller will inspect the object and
 * discover that's marshallable thanks to the annotation and so marshall it
 * using the externalizer class passed.
 *
 * It's common practice to include externalizer implementations within the
 * classes that they marshall/unmarshall as <code>public static classes</code>.
 * To make externalizer implementations easier to code and more typesafe, make
 * sure you define type <T> as the type of object that's being
 * marshalled/unmarshalled.
 *
 * Even though this way of defining externalizers is very user friendly, it has
 * some disadvantages:
 *
 * <ul>
 *    <li>Due to several constraints of the model, such as support different
 *    versions of the same class or the need to marshall the Externalizer
 *    class, the payload sizes generated via this method are not the most
 *    efficient.</li>
 *    <li>This model requires for the marshalled class to be annoated with
 *    {@link SerializeWith} but a user might need to provide an Externalizer
 *    for a class for which source code is not available, or for any other
 *    constraints, it cannot be modified.</li>
 *    <li>The use of annotations by this model might be limiting for framework
 *    developers or service providers that try to abstract lower level
 *    details, such as the marshalling layer, away from the user.</li>
 * </ul>
 *
 * If you're affected by any of these disadvantages, an alternative mechanism
 * to provide externalizers is available via {@link AdvancedExternalizer}.
 * More details can be found in this interface's javadoc.
 *
 * Please note that even though Externalizer is marked as {@link Serializable},
 * the need to marshall the externalizer is only really needed when developing
 * user friendly externalizers (using {@link SerializeWith}). {@link AdvancedExternalizer}
 * instances do not require the externalizer to be serializable since the
 * externalizer itself is not marshalled.
 *
 * Even though it's not strictly necessary, to avoid breaking compatibility
 * with old clients, {@link Externalizer} implements {@link Serializable} but
 * this requirement is only needed for those user friendly externalizers.
 * There's a chance that in future major releases {@link Externalizer} won't
 * extend {@link Serializable} any more, hence we strongly recommend that any
 * user-friendly externalizer users mark their externalizer implementations as
 * either {@link Serializable} or {@link java.io.Externalizable}.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
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
