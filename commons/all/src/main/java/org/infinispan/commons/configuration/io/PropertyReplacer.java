package org.infinispan.commons.configuration.io;

import java.util.Properties;

import org.infinispan.commons.util.StringPropertyReplacer;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface PropertyReplacer {
   PropertyReplacer DEFAULT = new Default();

   String replaceProperties(final String string, final Properties props);

   class Default implements PropertyReplacer {
      private Default() {
      }

      @Override
      public String replaceProperties(String string, Properties props) {
         return StringPropertyReplacer.replaceProperties(string, props);
      }
   }
}
