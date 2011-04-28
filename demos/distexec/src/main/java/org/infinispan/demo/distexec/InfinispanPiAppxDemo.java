package org.infinispan.demo.distexec;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedExecutorService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * 
 * Infinispan distributed executors demo using pi approximation.
 * 
 */
public class InfinispanPiAppxDemo {

   private static final String DEFAULT_CONFIG_FILE = "jgroups-s3_ping-aws.xml";
   private static final int DEFAULT_NUM_POINTS = 50000000;

   
   public static void main(String[] args) throws Exception {

      SimpleJSAP jsap = new SimpleJSAP("PiAppxDemo",
               "Approximate Pi using Infinispan DistributedExecutorService ", new Parameter[] {
                        new FlaggedOption("configFile", JSAP.STRING_PARSER, DEFAULT_CONFIG_FILE,
                                 JSAP.NOT_REQUIRED, 'c',"configFile",
                                 "Infinispan transport config file"),
                        new FlaggedOption("nodeType", JSAP.STRING_PARSER, "slave", JSAP.REQUIRED,
                                 't', "nodeType", "Node type as either master or slave"),
                        new FlaggedOption("numPoints", JSAP.INTEGER_PARSER,
                                 String.valueOf(DEFAULT_NUM_POINTS), JSAP.REQUIRED, 'n',
                                 "numPoints", "Total number of darts to shoot"),
                        new FlaggedOption("ispnConfigFile", JSAP.STRING_PARSER, null,
                                 JSAP.NOT_REQUIRED, 'i', "ispnConfigFile",
                                 "Infinispan cache configuration file") });

      JSAPResult config = jsap.parse(args);
      if (!config.success() || jsap.messagePrinted()) {
         Iterator<?> messageIterator = config.getErrorMessageIterator();
         while (messageIterator.hasNext()) {
            System.err.println(messageIterator.next());
         }
         System.err.println(jsap.getHelp());
         return;
      }

      String nodeType = config.getString("nodeType");
      boolean isMaster = nodeType != null && nodeType.equals("master");
      String transportConfig = config.getString("configFile");
      String ispnConfigFile = config.getString("ispnConfigFile");
      int numPoints = config.getInt("numPoints");
      System.out.println("Starting Infinispan node using transport config file " + transportConfig);
      
      if(ispnConfigFile != null)
         System.out.println("Starting Infinispan node using Infinispan config file " + ispnConfigFile);
      
      CacheBuilder cb = new CacheBuilder(ispnConfigFile, transportConfig);
      EmbeddedCacheManager cacheManager = cb.getCacheManager();
      Cache<Object, Object> cache = cacheManager.getCache();

      Transport transport = cache.getAdvancedCache().getRpcManager().getTransport();
      int numServers = transport.getMembers().size();

      if (isMaster) {
         System.out.println("Member " + transport.getAddress()
                  + " joined as master and its view is " + transport.getMembers()
                  + ", starting Pi appx across " + numServers + " machines");

         int numberPerWorker = numPoints / numServers;

         DistributedExecutorService des = new DefaultExecutorService(cache);
         long start = System.currentTimeMillis();
         List<Future<Integer>> results = des.submitEverywhere(new CircleTest(numberPerWorker));
         int insideCircleCount = 0;
         for (Future<Integer> f : results) {
            insideCircleCount += f.get();
         }
         double appxPi = 4.0 * insideCircleCount / numPoints;

         long computationTime = (System.currentTimeMillis() - start);
         System.out.println("PI appx is " + appxPi + " completed in " + computationTime + " ms");
         cacheManager.stop();
      } else {
         System.out.println("Member " + transport.getAddress()
                  + " joined as slave and its view is " + transport.getMembers() + ", waiting....");
      }
   }

   private static class CircleTest implements Callable<Integer>, Serializable {

      /** The serialVersionUID */
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
}
