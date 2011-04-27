/* 
 * JBoss, Home of Professional Open Source 
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tag. All rights reserved. 
 * See the copyright.txt in the distribution for a 
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use, 
 * modify, copy, or redistribute it subject to the terms and conditions 
 * of the GNU Lesser General Public License, v. 2.1. 
 * This program is distributed in the hope that it will be useful, but WITHOUT A 
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A 
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details. 
 * You should have received a copy of the GNU Lesser General Public License, 
 * v.2.1 along with this distribution; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, 
 * MA  02110-1301, USA.
 */
package org.infinispan.demo.distexec.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.infinispan.Cache;
import org.infinispan.demo.distexec.CacheBuilder;
import org.infinispan.distexec.mapreduce.Collator;
import org.infinispan.distexec.mapreduce.Collector;
import org.infinispan.distexec.mapreduce.MapReduceTask;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.FileLookup;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * Infinispan MapReduceTask demo
 * 
 * 
 * @author Vladimir Blagojevic
 */
public class WordCountDemo {

   private static final String DEFAULT_CONFIG_FILE = "jgroups-s3_ping-aws.xml";

   public static void main(String[] args) throws Exception {
     

      SimpleJSAP jsap = new SimpleJSAP(
               "WordCountDemo",
               "Count words in Infinispan cache usin MapReduceTask ",
               new Parameter[] {
                        new FlaggedOption("configFile", JSAP.STRING_PARSER, DEFAULT_CONFIG_FILE,
                                 JSAP.NOT_REQUIRED, 'c', "configFile",
                                 "Infinispan transport config file"),
                        new FlaggedOption("nodeType", JSAP.STRING_PARSER, "slave", JSAP.REQUIRED,
                                 't', "nodeType", "Node type as either master or slave"),
                        new FlaggedOption("textFile", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED,
                                 'f', "textFile", "Input text file to distribute onto grid"),
                        new FlaggedOption("ispnConfigFile", JSAP.STRING_PARSER, null,
                                 JSAP.NOT_REQUIRED, 'i', "ispnConfigFile",
                                 "Infinispan cache configuration file"),
                                 new FlaggedOption("kthWord", JSAP.INTEGER_PARSER, "15",
                                          JSAP.NOT_REQUIRED, 'k', "kthWord",
                                          "Kth most frequent word")});

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
      int kthWord = config.getInt("kthWord");
      System.out.println("Starting Infinispan node using transport config file " + transportConfig);

      if (ispnConfigFile != null)
         System.out.println("Starting Infinispan node using Infinispan config file " + ispnConfigFile);
      
      String textFile = config.getString("textFile");
      BufferedReader bufferedReader = null;
      if(textFile != null){
         FileLookup fl = new FileLookup();
         InputStream lookupFile = fl.lookupFile(textFile);
         if(lookupFile == null) {
            System.err.println("Intended input text file " +  textFile + " not found. Make sure it is on classpath");
            return;
         }                 
         bufferedReader = new BufferedReader(new FileReader(textFile));         
      }
      

      CacheBuilder cb = new CacheBuilder(ispnConfigFile, transportConfig);
      EmbeddedCacheManager cacheManager = cb.getCacheManager();
      Cache<String, String> cache = cacheManager.getCache();
      
      //chunk and insert into cache
      int chunkSize = 10; // 10K
      int chunkId = 0;
      if(bufferedReader != null){
         CharBuffer cbuf = CharBuffer.allocate(1024 * chunkSize);
         while (bufferedReader.read(cbuf) >= 0) {
            Buffer buffer = cbuf.flip();
            String textChunk = buffer.toString();
            cache.put(textFile + (chunkId++), textChunk);
            cbuf.clear();
         }
      }

      Transport transport = cache.getAdvancedCache().getRpcManager().getTransport();
      int numServers = transport.getMembers().size();

      if (isMaster) {
         System.out.println("Member " + transport.getAddress()
                  + " joined as master and its view is " + transport.getMembers()
                  + ", starting MapReduceTask across " + numServers + " machines");

         long start = System.currentTimeMillis();
         MapReduceTask<String, String, String, Integer> t = new MapReduceTask<String, String, String, Integer>(cache);
         t.mappedWith(new WordCountMapper()).reducedWith(new WordCountReducer());
         Entry<String, Integer> kthMostFrequentEntry = t.execute(new KFrequentWordCollator(kthWord));
         if (kthMostFrequentEntry != null) {
            System.out.println("Kth(where k=" + kthWord + ") most frequent word is "
                     + kthMostFrequentEntry.getKey() + " occurring "
                     + kthMostFrequentEntry.getValue() + " times. Found in "
                     + (System.currentTimeMillis() - start) + " ms");
         } else {
            System.out.println("Kth(where k=" + kthWord + ") most frequent word is too large for this data set. Try smaller k");
         }
         cacheManager.stop();
      } else {
         System.out.println("Member " + transport.getAddress()
                  + " joined as slave and its view is " + transport.getMembers() + ", waiting....");
      }
   }
   
   static class KFrequentWordCollator implements Collator<String, Integer, Entry<String, Integer>> {

      private final int kthFrequentWord;

      public KFrequentWordCollator(int kthFrequentWord) {
         super();
         if (kthFrequentWord < 0)
            throw new IllegalArgumentException("kthFrequentWord can not be less than 0");
         this.kthFrequentWord = kthFrequentWord;
      }

      @Override
      public Entry<String, Integer> collate(Map<String, Integer> reducedResults) {
         Set<Entry<String, Integer>> entrySet = reducedResults.entrySet();
         ArrayList<Entry<String, Integer>> l = new ArrayList<Entry<String, Integer>>();
         l.addAll(entrySet);
         Collections.sort(l, new Comparator<Entry<String, Integer>>() {

            @Override
            public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
               return o1.getValue() < o2.getValue() ? 1 : o1.getValue() > o2.getValue() ? -1 : 0;
            }
         });
         if(kthFrequentWord < l.size()){
            return l.get(kthFrequentWord-1); //account for zero index List
         }
         return null;        
      }
   }

   static class WordCountMapper implements Mapper<String, String, String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = -5943370243108735560L;

      @Override
      public void map(String key, String value, Collector<String, Integer> c) {
         StringTokenizer tokens = new StringTokenizer(value);
         while (tokens.hasMoreElements()) {
            String s = (String) tokens.nextElement();
            c.emit(s, 1);
         }
      }
   }

   static class WordCountReducer implements Reducer<String, Integer> {
      /** The serialVersionUID */
      private static final long serialVersionUID = 1901016598354633256L;

      @Override
      public Integer reduce(String key, Iterator<Integer> iter) {
         int sum = 0;
         while (iter.hasNext()) {
            Integer i = (Integer) iter.next();
            sum += i;
         }
         return sum;
      }
   }
}
