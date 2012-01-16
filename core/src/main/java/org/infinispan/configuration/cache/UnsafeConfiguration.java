package org.infinispan.configuration.cache;

/**
 * Allows you to tune various unsafe or non-standard characteristics. Certain operations such as
 * Cache.put() that are supposed to return the previous value associated with the specified key
 * according to the java.util.Map contract will not fulfill this contract if unsafe toggle is turned
 * on. Use with care. See details at <a
 * href="https://docs.jboss.org/author/display/ISPN/Technical+FAQs">Technical FAQ</a>
 * 
 * @author pmuir
 * 
 */
public class UnsafeConfiguration {

   private final boolean unreliableReturnValues;

   UnsafeConfiguration(boolean unreliableReturnValues) {
      this.unreliableReturnValues = unreliableReturnValues;
   }

   public boolean unreliableReturnValues() {
      return unreliableReturnValues;
   }

}
