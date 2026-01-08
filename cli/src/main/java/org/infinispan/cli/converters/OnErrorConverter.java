package org.infinispan.cli.converters;

import org.infinispan.cli.completers.OnErrorCompleter;

public class OnErrorConverter extends EnumConverter<OnErrorCompleter.OnError> {
   public OnErrorConverter() {
      super(OnErrorCompleter.OnError.class);
   }
}
