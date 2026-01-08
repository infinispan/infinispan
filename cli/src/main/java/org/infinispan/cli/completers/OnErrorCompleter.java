package org.infinispan.cli.completers;

public class OnErrorCompleter extends EnumCompleter<OnErrorCompleter.OnError> {

   public enum OnError {
      IGNORE,
      FAIL_FAST,
      FAIL_AT_END;
   }

   public OnErrorCompleter() {
      super(OnError.class);
   }
}
