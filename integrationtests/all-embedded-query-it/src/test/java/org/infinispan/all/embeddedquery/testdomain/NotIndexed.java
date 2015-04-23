package org.infinispan.all.embeddedquery.testdomain;

import java.io.Serializable;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexed implements Serializable {

   public String notIndexedField;

   public NotIndexed(String notIndexedField) {
      this.notIndexedField = notIndexedField;
   }
}
