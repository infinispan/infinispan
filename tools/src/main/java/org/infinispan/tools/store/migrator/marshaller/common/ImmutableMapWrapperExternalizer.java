package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.Immutables;

public class ImmutableMapWrapperExternalizer extends AbstractMigratorExternalizer<Map> {

   public ImmutableMapWrapperExternalizer() {
      super(Immutables.ImmutableMapWrapper.class, Ids.IMMUTABLE_MAP);
   }

   @Override
   public Map readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return Immutables.immutableMapWrap(MarshallUtil.unmarshallMap(input, HashMap::new));
   }
}
