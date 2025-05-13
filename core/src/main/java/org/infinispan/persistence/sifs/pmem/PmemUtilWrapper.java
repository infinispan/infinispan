package org.infinispan.persistence.sifs.pmem;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.channels.FileChannel;

import io.mashona.logwriting.PmemUtil;

/**
 * This class is here solely for the purpose of encapsulating the {@link PmemUtil} class so we do not load it unless
 * necessary, allowing this to be an optional dependency. Any code that invokes a method in this class should first
 * check if the {@link PmemUtil} can be loaded via {@link Class#forName(String)} otherwise a {@link ClassNotFoundException}
 * may be thrown when loading this class.
 */
public class PmemUtilWrapper {
   /**
    * Same as {@link PmemUtil#pmemChannelFor(File, int, boolean, boolean)}.
    */
   public static FileChannel pmemChannelFor(File file, int length, boolean create, boolean readSharedMetadata) throws FileNotFoundException {
      return PmemUtil.pmemChannelFor(file, length, create, readSharedMetadata);
   }
}
