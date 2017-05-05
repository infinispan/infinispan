package org.infinispan.functional.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.api.functional.MetaParam;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

public final class MetaParamsExternalizer extends AbstractExternalizer<MetaParams> {
   @Override
   public void writeObject(ObjectOutput oo, MetaParams o) throws IOException {
      oo.writeInt(o.size());
      for (MetaParam meta : o) oo.writeObject(meta);
   }

   @Override
   public MetaParams readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      int length = input.readInt();
      MetaParam[] metas = new MetaParam[length];
      for (int i = 0; i < length; i++)
         metas[i] = (MetaParam) input.readObject();

      return MetaParams.of(metas);
   }

   @Override
   public Set<Class<? extends MetaParams>> getTypeClasses() {
      return Util.<Class<? extends MetaParams>>asSet(MetaParams.class);
   }

   @Override
   public Integer getId() {
      return Ids.META_PARAMS;
   }
}
