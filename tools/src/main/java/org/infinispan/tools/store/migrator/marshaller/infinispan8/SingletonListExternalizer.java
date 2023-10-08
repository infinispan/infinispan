package org.infinispan.tools.store.migrator.marshaller.infinispan8;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collections;
import java.util.List;

import org.infinispan.commons.util.Util;
import org.infinispan.tools.store.migrator.marshaller.common.AbstractMigratorExternalizer;

import net.jcip.annotations.Immutable;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Immutable
class SingletonListExternalizer extends AbstractMigratorExternalizer<List<?>> {

   public SingletonListExternalizer() {
      super(Util.loadClass("java.util.Collections$SingletonList", null), ExternalizerTable.SINGLETON_LIST);
   }

   @Override
   public List<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return Collections.singletonList(input.readObject());
   }
}
