package org.infinispan.server.hotrod.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * Functions and Predicated and their externalizer.
 * <p>
 * To be used on cache streams.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class TxFunctions {

   public static final AdvancedExternalizer<IdFunction> EXTERNALIZER = new Externalizer();

   private static final byte TX_STATE_GTX_FUNCTION_ID = 0x01;
   private static final byte EQUALS_GTX_ID = 0x02;

   private static final TxStateToGlobalTransaction TX_STATE_GTX_FUNCTION = new TxStateToGlobalTransaction();

   private TxFunctions() {
   }

   static Function<TxState, GlobalTransaction> mapTxStateToGlobalTransaction() {
      return TX_STATE_GTX_FUNCTION;
   }

   static Predicate<GlobalTransaction> equalsGlobalTransaction(GlobalTransaction gtx) {
      return new EqualsGlobalTransaction(Objects.requireNonNull(gtx));
   }

   private interface IdFunction {
      byte getId();
   }

   private static class EqualsGlobalTransaction implements IdFunction, Predicate<GlobalTransaction> {

      private final GlobalTransaction gtxToTest;

      private EqualsGlobalTransaction(GlobalTransaction gtxToTest) {
         this.gtxToTest = gtxToTest;
      }

      @Override
      public boolean test(GlobalTransaction globalTransaction) {
         return gtxToTest.equals(globalTransaction);
      }

      @Override
      public byte getId() {
         return EQUALS_GTX_ID;
      }
   }

   private static class TxStateToGlobalTransaction implements IdFunction, Function<TxState, GlobalTransaction> {

      @Override
      public GlobalTransaction apply(TxState txState) {
         return txState.getGlobalTransaction();
      }

      @Override
      public byte getId() {
         return TX_STATE_GTX_FUNCTION_ID;
      }
   }

   private static class Externalizer implements AdvancedExternalizer<IdFunction> {

      @Override
      public Set<Class<? extends IdFunction>> getTypeClasses() {
         return new HashSet<>(Arrays.asList(TxStateToGlobalTransaction.class, EqualsGlobalTransaction.class));
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.TX_FUNCTIONS;
      }

      @Override
      public void writeObject(ObjectOutput output, IdFunction object) throws IOException {
         output.writeByte(object.getId());
         if (object.getId() == EQUALS_GTX_ID) {
            output.writeObject(((EqualsGlobalTransaction) object).gtxToTest);
         }
      }

      @Override
      public IdFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         switch (input.readByte()) {
            case TX_STATE_GTX_FUNCTION_ID:
               return TX_STATE_GTX_FUNCTION;
            case EQUALS_GTX_ID:
               return new EqualsGlobalTransaction((GlobalTransaction) input.readObject());
            default:
               throw new IllegalStateException();
         }
      }
   }

}
