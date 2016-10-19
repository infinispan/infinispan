package org.infinispan.query.dsl.embedded.testdomain;

import java.io.Serializable;

import org.infinispan.marshall.core.ExternalPojo;

/**
 * @author anistor@redhat.com
 * @since 7.2
 */
public class NotIndexed implements Serializable, ExternalPojo {

   public String notIndexedField;

   public NotIndexed(String notIndexedField) {
      this.notIndexedField = notIndexedField;
   }
}
