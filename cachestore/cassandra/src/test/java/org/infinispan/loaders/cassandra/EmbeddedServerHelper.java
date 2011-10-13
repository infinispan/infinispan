package org.infinispan.loaders.cassandra;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.thrift.CassandraDaemon;
import org.apache.thrift.transport.TTransportException;

/**
 * Taken from Hector (MIT license).
 * 
 * Copyright (c) 2010 Ran Tavory
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 */
public class EmbeddedServerHelper {
   private static final String TMP = System.getProperty("java.io.tmpdir")+File.separator+EmbeddedServerHelper.class.getPackage().getName()+"-test";

   private final String yamlFile;
   static CassandraDaemon cassandraDaemon;

   public EmbeddedServerHelper() {
      this("/cassandra.yaml");
   }

   public EmbeddedServerHelper(String yamlFile) {
      this.yamlFile = yamlFile;
   }

   static ExecutorService executor = Executors.newSingleThreadExecutor();

   /**
    * Set embedded cassandra up and spawn it in a new thread.
    * 
    * @throws TTransportException
    * @throws IOException
    * @throws InterruptedException
    */
   public void setup() throws TTransportException, IOException, InterruptedException,
            ConfigurationException {
      // delete tmp dir first
      rmdir(TMP);
      // make a tmp dir and copy cassandra.yaml and log4j.properties to it
      copy("/log4j.properties", TMP);
      copy(yamlFile, TMP);
      System.setProperty("cassandra.config", "file:" + TMP + yamlFile);
      System.setProperty("log4j.configuration", "file:" + TMP + "/log4j.properties");
      System.setProperty("cassandra-foreground", "true");

      cleanupAndLeaveDirs();

      executor.execute(new CassandraRunner());
      try {
         TimeUnit.SECONDS.sleep(3);
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   public static void teardown() throws IOException {
      executor.shutdown();
      executor.shutdownNow();
      // delete tmp dir
      rmdir(TMP);
   }

   private static void rmdir(String dir) throws IOException {
      File dirFile = new File(dir);
      if (dirFile.exists()) {
         FileUtils.deleteRecursive(new File(dir));
      }
   }

   /**
    * Copies a resource from within the jar to a directory.
    * 
    * @param resource
    * @param directory
    * @throws IOException
    */
   private static void copy(String resource, String directory) throws IOException {
      mkdir(directory);
      InputStream is = EmbeddedServerHelper.class.getResourceAsStream(resource);
      String fileName = resource.substring(resource.lastIndexOf("/") + 1);
      File file = new File(directory + System.getProperty("file.separator") + fileName);
      OutputStream out = new FileOutputStream(file);
      byte buf[] = new byte[1024];
      int len;
      while ((len = is.read(buf)) > 0) {
         out.write(buf, 0, len);
      }
      out.close();
      is.close();
   }

   /**
    * Creates a directory
    * 
    * @param dir
    * @throws IOException
    */
   private static void mkdir(String dir) throws IOException {
      FileUtils.createDirectory(dir);
   }

   public static void cleanupAndLeaveDirs() throws IOException {
      mkdirs();
      cleanup();
      mkdirs();
      CommitLog.instance.resetUnsafe(); // cleanup screws w/ CommitLog, this
      // brings it back to safe state
   }

   public static void cleanup() throws IOException {
      // clean up commitlog
      String[] directoryNames = { DatabaseDescriptor.getCommitLogLocation(), };
      for (String dirName : directoryNames) {
         File dir = new File(dirName);
         if (!dir.exists())
            throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
         FileUtils.deleteRecursive(dir);
      }

      // clean up data directory which are stored as data directory/table/data
      // files
      for (String dirName : DatabaseDescriptor.getAllDataFileLocations()) {
         File dir = new File(dirName);
         if (!dir.exists())
            throw new RuntimeException("No such directory: " + dir.getAbsolutePath());
         FileUtils.deleteRecursive(dir);
      }
   }

   public static void mkdirs() {
      try {
         DatabaseDescriptor.createAllDirectories();
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
   }

   class CassandraRunner implements Runnable {
      @Override
      public void run() {
         cassandraDaemon = new CassandraDaemon();
         cassandraDaemon.activate();
      }
   }
}
