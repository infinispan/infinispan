package org.infinispan.tools.store.migrator.marshaller.common;

import java.util.Set;

public interface AdvancedExternalizer<T> extends Externalizer<T> {

   /**
    * The minimum ID which will be respected by Infinispan for user specified {@link AdvancedExternalizer} implementations.
    */
   int USER_EXT_ID_MIN = 2500;

   /**
    * Returns a collection of Class instances representing the types that this
    * AdvancedExternalizer can marshall.  Clearly, empty sets are not allowed.
    * The externalizer framework currently requires all individual types to be
    * listed since it does not make assumptions based on super classes or
    * interfaces.
    *
    * @return A set containing the Class instances that can be marshalled.
    */
   Set<Class<? extends T>> getTypeClasses();

   /**
    * Returns an integer that identifies the externalizer type. This is used
    * at read time to figure out which {@link AdvancedExternalizer} should read
    * the contents of the incoming buffer.
    *
    * Using a positive integer allows for very efficient variable length
    * encoding of numbers, and it's much more efficient than shipping
    * {@link AdvancedExternalizer} implementation class information around.
    * Negative values are not allowed.
    *
    * Implementers of this interface can use any positive integer as long as
    * it does not clash with any other identifier in the system.  You can find
    * information on the pre-assigned identifier ranges in
    * <a href="http://infinispan.org/docs/dev/user_guide/user_guide.html#preassigned_externalizer_id_ranges">here</a>.
    *
    * It's highly recommended that maintaining of these identifiers is done
    * in a centralized way and you can do so by making annotations reference
    * a set of statically defined identifiers in a separate class or
    * interface.  Such class/interface gives a global view of the identifiers
    * in use and so can make it easier to assign new ids.
    *
    * Implementors can optionally avoid giving a meaningful implementation to
    * this method (i.e. return null) and instead rely on XML or programmatic
    * configuration to provide the AdvancedExternalizer id.  If no id can be
    * determined via the implementation or XML/programmatic configuration, an
    * error will be reported.  If an id has been defined both via the
    * implementation and XML/programmatic configuration, the value defined via
    * XML/programmatic configuration will be used ignoring the other.
    *
    * @return A positive identifier for the AdvancedExternalizer.
    */
   Integer getId();

}
