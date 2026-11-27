package org.infinispan.cli.commands;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.testing.jupiter.tags.Cli;
import org.junit.jupiter.api.Test;

/**
 * @since 16.3
 **/
@Cli
public class PluginExecutableTest {

   @Test
   public void testKubectlPlugin() {
      assertTrue(CLI.isPluginExecutable("/usr/local/bin/kubectl-infinispan"));
      assertTrue(CLI.isPluginExecutable("kubectl-infinispan"));
      assertTrue(CLI.isPluginExecutable("C:\\Program Files\\kubectl\\kubectl-infinispan.exe"));
   }

   @Test
   public void testOpenShiftPlugin() {
      assertTrue(CLI.isPluginExecutable("/usr/local/bin/oc-infinispan"));
      assertTrue(CLI.isPluginExecutable("oc-infinispan"));
      assertTrue(CLI.isPluginExecutable("C:\\Program Files\\oc\\oc-infinispan.exe"));
   }

   @Test
   public void testStandaloneCli() {
      assertFalse(CLI.isPluginExecutable("/opt/infinispan/bin/infinispan-cli"));
      assertFalse(CLI.isPluginExecutable("-"));
      assertFalse(CLI.isPluginExecutable(""));
      // Neither kubectl nor oc themselves are the plugin
      assertFalse(CLI.isPluginExecutable("/usr/bin/kubectl"));
      assertFalse(CLI.isPluginExecutable("/usr/bin/oc"));
      // The name of a directory on the path must not enable the Kubernetes commands
      assertFalse(CLI.isPluginExecutable("/home/user/kubectl-plugins/infinispan-cli"));
   }
}
