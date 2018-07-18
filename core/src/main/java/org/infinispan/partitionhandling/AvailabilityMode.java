package org.infinispan.partitionhandling;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
* @author Mircea Markus
* @author Dan Berindei
* @since 7.0
*/
public enum AvailabilityMode {
   /** Regular operation mode */
   AVAILABLE,
   /** Data can not be safely accessed because of a network split or successive nodes leaving. */
   DEGRADED_MODE,
   ;

   public AvailabilityMode min(AvailabilityMode other) {
      if (this == DEGRADED_MODE || other == DEGRADED_MODE)
         return DEGRADED_MODE;
      return AVAILABLE;
   }

   private static final AvailabilityMode[] CACHED_VALUES = values();

   public static AvailabilityMode valueOf(int ordinal) {
      return CACHED_VALUES[ordinal];
   }

   public static class Externalizer extends AbstractExternalizer<AvailabilityMode> {
      @Override
      public Integer getId() {
         return Ids.AVAILABILITY_MODE;
      }

      @Override
      public Set<Class<? extends AvailabilityMode>> getTypeClasses() {
         return Util.<Class<? extends AvailabilityMode>>asSet(AvailabilityMode.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, AvailabilityMode AvailabilityMode) throws IOException {
         MarshallUtil.marshallEnum(AvailabilityMode, output);
      }

      @Override
      public AvailabilityMode readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return MarshallUtil.unmarshallEnum(input, AvailabilityMode::valueOf);
      }
   }
}
