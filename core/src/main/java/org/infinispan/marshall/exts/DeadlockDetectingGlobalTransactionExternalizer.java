package org.infinispan.marshall.exts;

import org.infinispan.transaction.xa.DeadlockDetectingGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransactionFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Externalizer for {@link DeadlockDetectingGlobalTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Externalizer implementation now within {@link DeadlockDetectingGlobalTransaction}
 */
public class DeadlockDetectingGlobalTransactionExternalizer extends GlobalTransactionExternalizer {

   public DeadlockDetectingGlobalTransactionExternalizer() {
      super();
      gtxFactory = new GlobalTransactionFactory(true);
   }

   @Override
   public void writeObject(ObjectOutput output, Object subject) throws IOException {
      super.writeObject(output, subject);
      DeadlockDetectingGlobalTransaction ddGt = (DeadlockDetectingGlobalTransaction) subject;
      output.writeLong(ddGt.getCoinToss());
   }

   @Override
   public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      DeadlockDetectingGlobalTransaction ddGt = (DeadlockDetectingGlobalTransaction) super.readObject(input);
      ddGt.setCoinToss(input.readLong());
      return ddGt;
   }
}
