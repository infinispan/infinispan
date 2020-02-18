package org.infinispan.server.test.core.persistence;

import java.util.Properties;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ExternalDatabase extends Database {
   public ExternalDatabase(String type, Properties properties) {
      super(type, properties);
   }

   @Override
   public void start() {
      // Do nothing
   }

   @Override
   public void stop() {
      // Do nothing
   }
}
