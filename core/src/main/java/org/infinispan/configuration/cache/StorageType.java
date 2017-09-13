package org.infinispan.configuration.cache;

import org.infinispan.configuration.parsing.Element;

/**
 * Enumeration defining the various storage types for the data container
 * @author wburns
 * @since 9.0
 */
public enum StorageType {
   OBJECT(Element.OBJECT),
   BINARY(Element.BINARY),
   OFF_HEAP(Element.OFFHEAP);

   Element element;
   StorageType(Element element) {
      this.element = element;
   }

   public Element getElement() {
      return element;
   }
}
