package org.infinispan.hash;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.hash.EquivalenceMurmurHash3;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * Enables injection of equivalence class via dependency mechanism.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class InjectableEquivalenceMurmurHash3 extends EquivalenceMurmurHash3 {

   public InjectableEquivalenceMurmurHash3() {}

   public InjectableEquivalenceMurmurHash3(Equivalence equivalence) {
      super(equivalence);
   }

   @Inject
   public void injectDependencies(Configuration configuration) {
      this.equivalence = configuration.dataContainer().keyEquivalence();
   }

   public static class Externalizer implements AdvancedExternalizer<InjectableEquivalenceMurmurHash3> {
      @Override
      public Set<Class<? extends InjectableEquivalenceMurmurHash3>> getTypeClasses() {
         return Util.asSet(InjectableEquivalenceMurmurHash3.class);
      }

      @Override
      public Integer getId() {
         return Ids.INJECTABLE_EQUIVALENCE_MURMUR_HASH3;
      }

      @Override
      public void writeObject(ObjectOutput output, InjectableEquivalenceMurmurHash3 object) throws IOException {
         output.writeObject(object.equivalence);
      }

      @Override
      public InjectableEquivalenceMurmurHash3 readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new InjectableEquivalenceMurmurHash3((Equivalence) input.readObject());
      }
   }
}
