package org.infinispan.client.hotrod.query;

import static org.testng.AssertJUnit.fail;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.query.remote.impl.logging.Log;

/**
 * @author anistor@redhat.com
 * @since 9.3
 */
public final class RemoteQueryTestUtils {

   private static final Log log = LogFactory.getLog(RemoteQueryTestUtils.class, Log.class);

   /**
    * Logs the Protobuf schema errors (if any) and fails the test if there are schema errors.
    */
   public static void checkSchemaErrors(RemoteCache<String, String> metadataCache) {
      if (metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX)) {
         // The existence of this key indicates there are errors in some files
         String files = metadataCache.get(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX);
         for (String fname : files.split("\n")) {
            String errorKey = fname + ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX;
            log.errorf("Found errors in Protobuf schema file: %s\n%s\n", fname, metadataCache.get(errorKey));
         }

         fail("There are errors in the following Protobuf schema files:\n" + files);
      }
   }
}
