package org.infinispan.marshall.exts;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.topology.RebalancingStatus;
import org.infinispan.xsite.BackupSender;

/**
 * An externalizer for internal enum types.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
public class EnumExternalizer implements AdvancedExternalizer<Enum<?>> {

   public static final EnumExternalizer INSTANCE = new EnumExternalizer();

   @Override
   public Set<Class<? extends Enum<?>>> getTypeClasses() {
      return Util.asSet(
            RebalancingStatus.class,
            BackupSender.BringSiteOnlineResponse.class,
            BackupSender.TakeSiteOfflineResponse.class
      );
   }

   @Override
   public Integer getId() {
      return Ids.INTERNAL_ENUMS;
   }

   @Override
   public void writeObject(ObjectOutput output, Enum<?> e) throws IOException {
      output.writeObject(e.getClass());
      output.writeUTF(e.name());
   }

   @Override
   public Enum<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Class enumClass = (Class) input.readObject();
      String name = input.readUTF();
      return Enum.valueOf(enumClass, name);
   }
}
