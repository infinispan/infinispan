package org.infinispan.commons.configuration.io;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.1
 **/
public interface ConfigurationReaderContext {
   void handleAnyElement(ConfigurationReader reader);

   void handleAnyAttribute(ConfigurationReader reader, int i);
}
