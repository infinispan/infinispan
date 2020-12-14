package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationSchemaVersion {
   String getURI();

   int getMajor();

   int getMinor();

   boolean since(int major, int minor);
}
