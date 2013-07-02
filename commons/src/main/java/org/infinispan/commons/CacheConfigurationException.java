package org.infinispan.commons;

import java.util.ArrayList;
import java.util.List;

/**
 * An exception that represents an error in the configuration.  This could be a parsing error or a logical error
 * involving clashing configuration options or missing mandatory configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 *
 * @since 4.0
 */
public class CacheConfigurationException extends CacheException {

   /** The serialVersionUID */
   private static final long serialVersionUID = -7103679310393205388L;
   private List<String> erroneousAttributes = new ArrayList<String>(4);

   public CacheConfigurationException(Exception e) {
      super(e);
   }

   public CacheConfigurationException(String string) {
      super(string);
   }

   public CacheConfigurationException(String string, String erroneousAttribute) {
      super(string);
      erroneousAttributes.add(erroneousAttribute);
   }

   public CacheConfigurationException(String string, Throwable throwable) {
      super(string, throwable);
   }

   public List<String> getErroneousAttributes() {
      return erroneousAttributes;
   }

   public void addErroneousAttribute(String s) {
      erroneousAttributes.add(s);
   }
}
