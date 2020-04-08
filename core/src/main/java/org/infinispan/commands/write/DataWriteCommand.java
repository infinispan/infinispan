package org.infinispan.commands.write;

import org.infinispan.commands.DataCommand;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {

   PrivateMetadata getInternalMetadata();

   void setInternalMetadata(PrivateMetadata internalMetadata);

   @Override
   default PrivateMetadata getInternalMetadata(Object key) {
      return key.equals(getKey()) ? getInternalMetadata() : null;
   }

   @Override
   default void setInternalMetadata(Object key, PrivateMetadata internalMetadata) {
      if (key.equals(getKey())) {
         setInternalMetadata(internalMetadata);
      }
   }
}
