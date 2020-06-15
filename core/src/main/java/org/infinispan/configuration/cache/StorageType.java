package org.infinispan.configuration.cache;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.parsing.Element;

/**
 * Enumeration defining the various storage types for the data container.
 *
 * @author wburns
 * @since 9.0
 */
public enum StorageType {

   /**
    * Objects are stored on heap as objects as provided. These are serialized across to other nodes and stored as
    * deserialized instances of the object. Equality is defined by the equals method of the implementation class.
    * @deprecated since 11.0, use {@link StorageType#HEAP} instead.
    */
   @Deprecated
   OBJECT(Element.OBJECT),

   /**
    * Objects are stored on heap. Equality is defined by the equals of the implementation class.
    * If the configured {@link MediaType} causes the storage to be byte[], then equality is defined by these
    * byte[] instances.
    */
   HEAP(Element.HEAP),

   /**
    * Entries are stored in Java heap but as byte[] instances. This mode has equality defined by the byte[] created from
    * the serialized from of the provided object.
    * @deprecated since 11.0, with no replacement. Use {@link #HEAP} and the cache's {@link EncodingConfiguration} to
    * define a {@link MediaType} that is byte[] or primitive based.
    */
   @Deprecated
   BINARY(Element.BINARY),

   /**
    * Entries are stored in internal memory off the normal Java heap. This mode has equality defined by the byte[]
    * created from the serialized form of the provided object.
    */
   OFF_HEAP(Element.OFF_HEAP);

   private final Element element;

   StorageType(Element element) {
      this.element = element;
   }

   public Element getElement() {
      return element;
   }

   public static StorageType forElement(String element) {
      return STORAGE_PER_ELEMENT.get(element);
   }

   public boolean canStoreReferences() {
      return this == HEAP || this == OBJECT;
   }

   private static final Map<String, StorageType> STORAGE_PER_ELEMENT = new HashMap<>(3);

   static {
      for (StorageType storageType : StorageType.values()) {
         STORAGE_PER_ELEMENT.put(storageType.element.getLocalName(), storageType);
      }
   }


}
