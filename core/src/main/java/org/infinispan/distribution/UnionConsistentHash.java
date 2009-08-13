package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = UnionConsistentHash.Externalizer.class, id = Ids.UNION_CONSISTENT_HASH)
public class UnionConsistentHash extends AbstractConsistentHash {

   ConsistentHash oldCH, newCH;

   public UnionConsistentHash(ConsistentHash oldCH, ConsistentHash newCH) {
      if ((oldCH instanceof UnionConsistentHash) || (newCH instanceof UnionConsistentHash))
         throw new CacheException("Expecting both newCH and oldCH to not be Unions!!  oldCH=[" + oldCH.getClass() + "] and newCH=[" + newCH.getClass() + "]");
      this.oldCH = oldCH;
      this.newCH = newCH;
   }

   public void setCaches(Collection<Address> caches) {
      // no op
   }

   public Collection<Address> getCaches() {
      return Collections.emptyList();
   }

   public List<Address> locate(Object key, int replCount) {
      Set<Address> addresses = new LinkedHashSet<Address>();
      addresses.addAll(oldCH.locate(key, replCount));
      addresses.addAll(newCH.locate(key, replCount));
      return Immutables.immutableListConvert(addresses);
   }

   public ConsistentHash getNewConsistentHash() {
      return newCH;
   }

   public ConsistentHash getOldConsistentHash() {
      return oldCH;
   }

   public static class Externalizer implements org.infinispan.marshall.Externalizer {

      public void writeObject(ObjectOutput output, Object object) throws IOException {
         UnionConsistentHash uch = (UnionConsistentHash) object;
         output.writeObject(uch.oldCH);
         output.writeObject(uch.newCH);
      }

      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new UnionConsistentHash((ConsistentHash) input.readObject(), (ConsistentHash) input.readObject());
      }
   }
}
