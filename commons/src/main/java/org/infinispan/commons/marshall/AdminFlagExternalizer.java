package org.infinispan.commons.marshall;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;

public class AdminFlagExternalizer extends AbstractExternalizer<CacheContainerAdmin.AdminFlag> {
   @Override
   public void writeObject(UserObjectOutput output, CacheContainerAdmin.AdminFlag flag) throws IOException {
      MarshallUtil.marshallEnum(flag, output);
   }

   @Override
   public CacheContainerAdmin.AdminFlag readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
      return MarshallUtil.unmarshallEnum(input, CacheContainerAdmin.AdminFlag::valueOf);
   }

   @Override
   public Integer getId() {
      return Ids.ADMIN_FLAG;
   }

   @Override
   public Set<Class<? extends CacheContainerAdmin.AdminFlag>> getTypeClasses() {
      return Collections.singleton(CacheContainerAdmin.AdminFlag.class);
   }
}
