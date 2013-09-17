package org.infinispan.config;

import org.infinispan.CacheException;

import java.util.ArrayList;
import java.util.List;

/**
 * An exception that represents an error in the configuration.  This could be a parsing error or a logical error
 * involving clashing configuration options or missing mandatory configuration elements.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @see Configuration
 * @see org.infinispan.manager.DefaultCacheManager
 * @since 4.0
 */
public class ConfigurationException extends CacheException {

   private static final long serialVersionUID = -5576382839360927955L;

   private List<String> erroneousAttributes = new ArrayList<String>(4);

   public ConfigurationException(Exception e) {
      super(e);
   }

   public ConfigurationException(String string) {
      super(string);
   }

   public ConfigurationException(String string, String erroneousAttribute) {
      super(string);
      erroneousAttributes.add(erroneousAttribute);
   }

   public ConfigurationException(String string, Throwable throwable) {
      super(string, throwable);
   }

   public List<String> getErroneousAttributes() {
      return erroneousAttributes;
   }

   public void addErroneousAttribute(String s) {
      erroneousAttributes.add(s);
   }
}
