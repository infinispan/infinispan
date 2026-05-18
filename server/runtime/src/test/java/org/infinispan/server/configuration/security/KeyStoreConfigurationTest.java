package org.infinispan.server.configuration.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Locale;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.server.configuration.AbstractConfigurationParserTest;
import org.infinispan.server.configuration.ServerConfigurationParserTest;
import org.infinispan.testing.jupiter.JupiterThreadTrackerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class KeyStoreConfigurationTest extends AbstractConfigurationParserTest {

   @RegisterExtension
   public static final JupiterThreadTrackerExtension tracker = new JupiterThreadTrackerExtension();

   @Override
   public String path() {
      return "configuration/invalid-alias/KeystoreInvalidAlias." + type.getSubType().toLowerCase(Locale.ROOT);
   }

   @Test
   public void shouldThrowOnInvalidAlias() {
      CacheConfigurationException e = assertThrows(CacheConfigurationException.class, this::loadAndParseConfiguration);
      assertEquals(exceptionMessage(), e.getMessage());
   }

   private String exceptionMessage() {
      return String.format("ISPN080069: Alias 'definitely-an-unknown-alias' not in keystore '%s'",
            ServerConfigurationParserTest.pathToKeystore());
   }
}
