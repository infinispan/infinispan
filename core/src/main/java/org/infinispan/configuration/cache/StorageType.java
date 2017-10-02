package org.infinispan.configuration.cache;

import org.infinispan.configuration.parsing.Element;

/**
 * Enumeration defining the various storage types for the data container
 * @author wburns
 * @since 9.0
 */
public enum StorageType {
   /**
    * Objects are stored on heap as objects as provided. These are serialized across to other nodes and stored as
    * deserialized instances of the object. Equality is defined by the equals method of the implementation class.
    */
   OBJECT(Element.OBJECT),
   /**
    * Entries are stored in Java heap but as byte[] instances. This mode has equality defined by the byte[] created from
    * the serialized from of the provided object.
    */
   BINARY(Element.BINARY),
   /**
    * Entries are stored off the normal Java heap. This mode has equality defined by the byte[] created from the
    * serialized from of the provided object.
    */
   OFF_HEAP(Element.OFFHEAP);

   Element element;
   StorageType(Element element) {
      this.element = element;
   }

   public Element getElement() {
      return element;
   }
}
