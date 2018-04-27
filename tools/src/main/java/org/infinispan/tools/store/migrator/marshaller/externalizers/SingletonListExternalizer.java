package org.infinispan.tools.store.migrator.marshaller.externalizers;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;

import net.jcip.annotations.Immutable;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
@Immutable
public class SingletonListExternalizer extends AbstractExternalizer<List<?>> {

   @Override
   public void writeObject(ObjectOutput output, List<?> list) throws IOException {
      output.writeObject(list.get(0));
   }

   @Override
   public List<?> readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      return Collections.singletonList(input.readObject());
   }

   @Override
   public Integer getId() {
      return LegacyIds.SINGLETON_LIST;
   }

   @Override
   public Set<Class<? extends List<?>>> getTypeClasses() {
      // This is loadable from any classloader
      return Util.<Class<? extends List<?>>>asSet(Util.<List<?>>loadClass("java.util.Collections$SingletonList", null));
   }
}
