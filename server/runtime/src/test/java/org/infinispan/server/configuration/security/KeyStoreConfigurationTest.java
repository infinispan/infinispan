package org.infinispan.server.configuration.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Locale;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.junit.JUnitThreadTrackerRule;
import org.infinispan.server.configuration.AbstractConfigurationParserTest;
import org.infinispan.server.configuration.ServerConfigurationParserTest;
import org.junit.ClassRule;
import org.junit.Test;

public class KeyStoreConfigurationTest extends AbstractConfigurationParserTest {

   @ClassRule
   public static final JUnitThreadTrackerRule tracker = new JUnitThreadTrackerRule();

   public KeyStoreConfigurationTest(MediaType type) {
      super(type);
   }

   @Override
   public String path() {
      return "configuration/invalid-alias/KeystoreInvalidAlias." + type.getSubType().toLowerCase(Locale.ROOT);
   }

   @Test
   public void shouldThrowOnInvalidAlias() {
      CacheConfigurationException e = assertThrows(CacheConfigurationException.class, this::loadAndParseConfiguration);
      assertTrue("Cause is: " + e.getCause(), e.getCause() instanceof CacheConfigurationException);
      assertEquals(exceptionMessage(), e.getCause().getMessage());
   }

   private String exceptionMessage() {
      return String.format("ISPN080069: Alias 'definitely-an-unknown-alias' not in keystore '%s'",
            ServerConfigurationParserTest.pathToKeystore());
   }
}
