package org.infinispan.query;

/**
 * Spits out the version number.
 *
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */
public class Version
{
   //version string - should correspond with the version in the pom
   private static final String version = "1.0.0-SNAPSHOT";
   public static void main(String[] args)
   {
      System.out.println("\nJBoss Cache Searchable Edition\nVersion:\t" + version + "\n\n");
   }
}
