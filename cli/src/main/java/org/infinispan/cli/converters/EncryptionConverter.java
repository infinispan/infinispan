package org.infinispan.cli.converters;

import org.infinispan.cli.completers.EncryptionCompleter;

public class EncryptionConverter extends EnumConverter<EncryptionCompleter.Encryption> {
   public EncryptionConverter() {
      super(EncryptionCompleter.Encryption.class);
   }
}
