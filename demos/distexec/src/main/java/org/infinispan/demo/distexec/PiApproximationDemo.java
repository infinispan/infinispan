package org.infinispan.demo.distexec;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import org.infinispan.Cache;
import org.infinispan.demo.Demo;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.util.Util;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.locks.LockSupport;

/**
 * Infinispan distributed executors demo using pi approximation.
 */
public class PiApproximationDemo extends Demo {
   private static final int DEFAULT_NUM_POINTS = 50000000;
   private int numPoints;

   public static void main(String... args) throws Exception {
      new PiApproximationDemo(args).run();
   }

   public PiApproximationDemo(String[] args) throws Exception {
      super(args);
      numPoints = commandLineOptions.getInt("numPoints");
   }

   public void run() throws Exception {
      // Step 1: start cache.
      Cache<String, String> cache = startCache();

      // Step 2: run Pi Approximation
      try {
         if (isMaster) {
            int numServers = cache.getCacheManager().getMembers().size();
            int numberPerWorker = numPoints / numServers;

            DistributedExecutorService des = new DefaultExecutorService(cache);

            long start = System.currentTimeMillis();
            List<Future<Integer>> results = des.submitEverywhere(new CircleTest(numberPerWorker));

            int insideCircleCount = 0;

            for (Future<Integer> f : results) insideCircleCount += f.get();

            double appxPi = 4.0 * insideCircleCount / numPoints;

            System.out.printf("Pi approximation is %s, computed in %s using %s nodes.%n", appxPi, Util.prettyPrintTime(System.currentTimeMillis() - start), numServers);

         } else {
            System.out.println("Slave node waiting for Map/Reduce tasks.  Ctrl-C to exit.");
            LockSupport.park();
         }
      } finally {
         cache.getCacheManager().stop();
      }
   }

   private static class CircleTest implements Callable<Integer>, Serializable {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 3496135215525904755L;

      private final int loopCount;

      public CircleTest(int loopCount) {
         this.loopCount = loopCount;
      }

      @Override
      public Integer call() throws Exception {
         int insideCircleCount = 0;
         for (int i = 0; i < loopCount; i++) {
            double x = Math.random();
            double y = Math.random();
            if (insideCircle(x, y))
               insideCircleCount++;
         }
         return insideCircleCount;
      }

      private boolean insideCircle(double x, double y) {
         return (Math.pow(x - 0.5, 2) + Math.pow(y - 0.5, 2)) <= Math.pow(0.5, 2);
      }
   }

   @Override
   protected SimpleJSAP buildCommandLineOptions() throws JSAPException {
      return new SimpleJSAP("PiApproximationDemo",
                            "Approximate Pi using Infinispan DistributedExecutorService ", new Parameter[]{
                  new FlaggedOption("configFile", JSAP.STRING_PARSER, "config-samples/distributed-ec2.xml",
                                    JSAP.NOT_REQUIRED, 'c', "configFile",
                                    "Infinispan config file"),
                  new FlaggedOption("nodeType", JSAP.STRING_PARSER, "slave", JSAP.REQUIRED,
                                    't', "nodeType", "Node type as either master or slave"),
                  new FlaggedOption("numPoints", JSAP.INTEGER_PARSER,
                                    String.valueOf(DEFAULT_NUM_POINTS), JSAP.REQUIRED, 'n',
                                    "numPoints", "Total number of darts to shoot"),
            });
   }
}
