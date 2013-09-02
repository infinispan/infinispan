package org.infinispan.compat;

import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.context.Flag;

/**
 * A type converter for cached keys and values. Given a key and value type,
 * implementations of this interface convert instances of those types into
 * target key and value type instances respectively.
 *
 * @param <K> cached key type
 * @param <V> cached value type
 * @param <KT> target key type
 * @param <VT> target value type
 */
public interface TypeConverter<K, V, KT, VT> {

   // The reason this interface takes both key and value types into account
   // is because given a key type, implementations can make clever decisions
   // on how to convert value types into target value types, as seen in
   // boxValue and unboxValue method definitions.

   /**
    * Covert a instance of cached key type into an instance of target key type.
    *
    * @param key cached key instance to convert
    * @return a converted key instance into target key type
    */
   KT boxKey(K key);

   /**
    * Covert a instance of cached key type into an instance of target key type.
    *
    * @param value cached value instance to convert
    * @return a converted value instance into target value type
    */
   VT boxValue(V value);

   /**
    * Convert back an instance of the target key type into an instance of
    * the cached key type.
    *
    * @param target target key type instance to convert back
    * @return an instance of the cached key type
    */
   K unboxKey(KT target);

   /**
    * Convert back an instance of the target value type into an instance of
    * the cached value type.
    *
    * @param target target value type instance to convert back
    * @return an instance of the cached value type
    */
   V unboxValue(VT target);

   /**
    * Indicates whether this type converter supports a particular type of
    * operation. This is used to route type conversion according to the origin
    * of the invocation
    *
    * @return true if operations with this flag should be routed to this type
    * converter, false otherwise
    */
   boolean supportsInvocation(Flag flag);

   /**
    * Marshaller to be used by the type converter to marshall/unmarshall contents.
    *
    * @param marshaller marshaller instance to be used.
    */
   void setMarshaller(Marshaller marshaller);

}
