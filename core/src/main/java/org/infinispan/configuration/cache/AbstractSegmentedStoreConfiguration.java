package org.infinispan.configuration.cache;

import java.io.File;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.spi.InitializationContext;

/**
 * Abstract store configuration that should be extended when the store configuration supports being segmented.
 * @author wburns
 * @since 9.4
 */
public abstract class AbstractSegmentedStoreConfiguration<T extends AbstractStoreConfiguration> extends AbstractStoreConfiguration {
   public AbstractSegmentedStoreConfiguration(Enum<?> element, AttributeSet attributes, AsyncStoreConfiguration async) {
      super(element, attributes, async);
   }

   /**
    * Method that is invoked each time a new store is created for a segment. This method should return a new
    * configuration that is configured to be persisted using the given segment.
    * @param segment the segment to use
    * @param ctx the initialization context from the persistence layer
    * @return the newly created configuration
    */
   public T newConfigurationFrom(int segment, InitializationContext ctx) {
      throw new UnsupportedOperationException("Please make sure you are implementing newConfigurationFrom(int, InitializationContext)");
   }

   /**
    * Transforms a file location to a segment based one. This is useful for file based stores where all you need to
    * do is append a segment to the name of the directory. If the provided location is a directory, that is that it is
    * terminated by in {@link File#separatorChar}, it will add a new directory onto that that is the segment. If the
    * location is a file, that is that it is not terminated by {@link File#separatorChar}, this will treat the location
    * as a directory and append a segment file in it. The underlying store may or may not preserve this and could still
    * turn the segment into a directory.
    * @param location original file location
    * @param segment the segment to append
    * @return string with the segment appended to the file location
    */
   public static String fileLocationTransform(String location, int segment) {
      if (location.endsWith(File.separator)) {
         return location.substring(0, location.length() - 1) + "-" + segment + File.separatorChar;
      }
      return location + File.separatorChar + segment;
   }
}
