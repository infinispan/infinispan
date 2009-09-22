package org.infinispan.query.helper;

import org.infinispan.query.test.Person;

import java.io.File;

/**
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */
public class IndexCleanUp
{

   public static void cleanUpIndexes()
   {
      Class[] knownClasses = {Person.class};
      for (Class c : knownClasses)
      {
         String dirName = c.getName();
         File file = new File(dirName);
         if (file.exists())
         {
            recursiveDelete(file);
         }
      }
   }

   private static void recursiveDelete(File f)
   {
      if (f.isDirectory())
      {
         File[] files = f.listFiles();
         for (File file : files) recursiveDelete(file);
      }
      else
      {
         f.delete();
      }
   }

}

