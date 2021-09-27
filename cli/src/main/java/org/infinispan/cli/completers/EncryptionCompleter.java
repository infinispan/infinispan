package org.infinispan.cli.completers;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class EncryptionCompleter extends EnumCompleter {

   public enum Encryption {
      None,
      Secret,
      Service;
   }

   public EncryptionCompleter() {
      super(Encryption.class);
   }
}
