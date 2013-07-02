package org.infinispan.remoting.responses;

import org.infinispan.commands.write.WriteCommand;

public class NoReturnValuesDistributionResponseGenerator extends DistributionResponseGenerator {
   @Override
   protected Response handleWriteCommand(WriteCommand wc, Object returnValue) {
      return wc.isSuccessful() ? null : UnsuccessfulResponse.INSTANCE;
   }
}
