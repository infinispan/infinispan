package org.infinispan.commands.write;

import org.infinispan.commands.DataCommand;
import org.infinispan.functional.impl.MetaParamsInternalMetadata;

/**
 * Mixes features from DataCommand and WriteCommand
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface DataWriteCommand extends WriteCommand, DataCommand {

   MetaParamsInternalMetadata getInternalMetadata();

   void setInternalMetadata(MetaParamsInternalMetadata internalMetadata);

   @Override
   default MetaParamsInternalMetadata getInternalMetadata(Object key) {
      return key.equals(getKey()) ? getInternalMetadata() : null;
   }

   @Override
   default void setInternalMetadata(Object key, MetaParamsInternalMetadata internalMetadata) {
      if (key.equals(getKey())) {
         setInternalMetadata(internalMetadata);
      }
   }
}
