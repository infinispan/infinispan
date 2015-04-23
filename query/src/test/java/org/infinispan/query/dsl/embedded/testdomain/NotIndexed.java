package org.infinispan.query.dsl.embedded.testdomain;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexed {

   public String notIndexedField;

   public NotIndexed(String notIndexedField) {
      this.notIndexedField = notIndexedField;
   }
}
