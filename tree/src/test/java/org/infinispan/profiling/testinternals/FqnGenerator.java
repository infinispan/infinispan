package org.infinispan.profiling.testinternals;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.infinispan.tree.Fqn;

/**
 * Helper class that will generate fqn's.
 *
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */


public class FqnGenerator {

   private static final Random r = new Random();

   public static Fqn createRandomFqn(int depth) {
      List<String> fqnElements = new ArrayList<String>(depth);
      for (int i = 0; i < depth; i++) fqnElements.add(Integer.toHexString(r.nextInt(Integer.MAX_VALUE)));
      return Fqn.fromList(fqnElements);
   }
}
