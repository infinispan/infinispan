package org.infinispan.commons.util;

import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;


/**
 * An {@link Enumeration} to {@link List} adapter.
 *
 * @author Pete Muir
 */
public class EnumerationList<T> extends ForwardingList<T> {

   // The enumeration as a list
   private final List<T> list = new LinkedList<>();

   /**
    * Constructor
    *
    * @param enumeration The enumeration
    */
   public EnumerationList(Enumeration<T> enumeration) {
      while (enumeration.hasMoreElements()) {
         list.add(enumeration.nextElement());
      }
   }

   @Override
   protected List<T> delegate() {
      return list;
   }
}
