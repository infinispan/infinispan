package org.infinispan.client.hotrod.impl.consistenthash;


import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Factory for {@link org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash} function. It will try to look
 * into the configuration for consistent hash definitions as follows:
 * consistent-hash.[version]=[fully qualified class implementing ConsistentHash]
 * e.g.
 * consistent-hash.1=org.infinispan.client.hotrod.impl.consistenthash.ConsitentHashV1
 * <p/>
 * If no CH function is defined for a certain version, then it will be defaulted to "org.infinispan.client.hotrod.impl.ConsistentHashV[version]".
 * E.g. if the server indicates that in use CH is version 1, and it is not defined within the configuration, it will be defaulted to
 * org.infinispan.client.hotrod.impl.ConsistentHashV1.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashFactory {

   private static Log log = LogFactory.getLog(ConsistentHashFactory.class);

   private final Map<Integer, String> version2ConsistentHash = new HashMap<Integer, String>();

   public void init(Properties props) {
      for (String propName : props.stringPropertyNames()) {
         if (propName.indexOf("consistent-hash") >= 0) {
            if (log.isTraceEnabled()) log.trace("Processing consistent hash: " + propName);
            String versionString = propName.substring("consistent-hash.".length());
            int version = Integer.parseInt(versionString);
            String hashFunction = props.getProperty(propName);
            version2ConsistentHash.put(version, hashFunction);
            if (log.isTraceEnabled()) {
               log.trace("Added consistent hash version " + version + ": " + hashFunction);
            }
         }
      }
   }

   public ConsistentHash newConsistentHash(int version) {
      String hashFunctionClass = version2ConsistentHash.get(version);
      if (hashFunctionClass == null) {
         if (log.isTraceEnabled()) log.trace("No hash function configured for version " + version);
         hashFunctionClass = ConsistentHashFactory.class.getPackage().getName() + ".ConsistentHashV" + version;
         if (log.isTraceEnabled()) log.trace("Trying to use default value: " + hashFunctionClass);
      }
      return (ConsistentHash) Util.getInstance(hashFunctionClass);
   }

   public Map<Integer, String> getVersion2ConsistentHash() {
      return Collections.unmodifiableMap(version2ConsistentHash);
   }
}
