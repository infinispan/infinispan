/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.demos.directory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @version $Id$
 */
public class FilesystemDirectory {
    EmbeddedCacheManager manager;
    Cache<String,byte[]> cache;


    public void start() throws Exception {
      manager = new DefaultCacheManager(GlobalConfigurationBuilder.defaultClusteredBuilder().build());      
      manager.defineConfiguration("dirCache",
               new ConfigurationBuilder().clustering().cacheMode(CacheMode.DIST_ASYNC).clustering()
                        .hash().numOwners(1).clustering().l1().enable().build());
      cache = manager.getCache("dirCache");

        boolean looping=true;
        while(looping) {
            int ch=Util.keyPress("[1] put dir [2] remove dir [3] dump [4] get [5] verify [x] exit");
            switch(ch) {
                case '1':
                    String dir=Util.readStringFromStdin("dir: ");
                    putDir(dir);
                    break;
                case '2':
                    dir=Util.readStringFromStdin("dir: ");
                    removeDir(dir);
                    break;
                case '3':
                    int count=0;
                    long bytes=0;
                    for(Map.Entry<String,byte[]> entry: cache.entrySet()) {
                        System.out.println(entry.getKey() + ": " + entry.getValue().length + " bytes");
                        count++;
                        bytes+=entry.getValue().length;
                    }
                    System.out.println(count + " files and " + Util.printBytes(bytes));
                    break;
                case '4':
                    String key=Util.readStringFromStdin("key: ");
                    byte[] val=cache.get(key);
                    if(val == null)
                        System.out.println("value for " + key + " not found");
                    else
                        System.out.println("val=" + val.length + " bytes");
                    break;
                case '5':
                    dir=Util.readStringFromStdin("dir: ");
                    verifyDir(dir);
                    break;
                case 'x':
                case 'X':
                    looping=false;
                    break;
                case -1:
                    looping=false;
                    break;
            }
        }
        cache.stop();
    }

    private void verifyDir(String dir) {
        if(dir == null)
            return;

        File root=new File(dir);
        if(!root.exists()) {
            System.err.println("Directory " + dir + " doesn't exist");
            return;
        }

        int count=verifyDir(root);
        System.out.println("OK: verified that " + root + " has " + count +
                " files in the file system and in the cache, and that their sizes match");
    }

    private int verifyDir(File dir) {
        File[] files=dir.listFiles();
        if(files == null)
            return 0;
        int count=0;
        for(File file: files) {
            if(file.isDirectory())
                count+=verifyDir(file);
            else if(file.isFile()) {
                try {
                    String key=file.getPath();
                    byte[] val=getContents(file);
                    byte[] actual_val_in_cache=cache.get(key);
                    if(!Arrays.equals(val, actual_val_in_cache)) {
                        System.err.println("for key " + key + ": the file has " + val.length +
                                " bytes in the file system, but " + actual_val_in_cache.length + " bytes in the cache");
                    }
                    count++;
                }
                catch(Throwable t) {
                    System.err.println("failed verifying " + file);
                }
            }
        }
        return count;
    }

    private void removeDir(String dir) {
        if(dir == null)
            return;

        File root=new File(dir);
        if(!root.exists()) {
            System.err.println("Directory " + dir + " doesn't exist");
            return;
        }

        int count=removeDir(root);
        System.out.println("removed " + count + " keys");
    }


    private int removeDir(File dir) {
        File[] files=dir.listFiles();
        if(files == null)
            return 0;
        int count=0;
        for(File file: files) {
            if(file.isDirectory())
                count+=removeDir(file);
            else if(file.isFile()) {
                String key=file.getPath();
                cache.remove(key);
                count++;
            }
        }
        return count;
    }

    private void putDir(String dir) {
        File root=new File(dir);
        if(!root.exists()) {
            System.err.println("Directory " + dir + " doesn't exist");
            return;
        }

        int count=putDir(root);
        System.out.println("added " + count + " files");
    }

    private int putDir(File dir) {
        File[] files=dir.listFiles();
        if(files == null)
            return 0;
        int count=0;
        for(File file: files) {
            if(file.isDirectory())
                count+=putDir(file);
            else if(file.isFile()) {
                String key=file.getPath();
                byte[] val=null;
                try {
                    val=getContents(file);
                    cache.put(key, val);
                    count++;
                }
                catch(Throwable e) {
                    System.err.println("failed reading contents of " + file);
                }
            }
        }
        return count;
    }

    private static byte[] getContents(File file) throws IOException {
        InputStream in;
        in=new FileInputStream(file);
        try {
            int length=(int)file.length();
            byte[] buffer=new byte[length];
            int read=in.read(buffer, 0, length);
            if(read != length)
                throw new IllegalStateException("length was " + length + ", but only " + read + " bytes were read");
            return buffer;
        }
        finally {
            Util.close(in);
        }
    }


    public static void main(String[] args) throws Exception {
        FilesystemDirectory test=new FilesystemDirectory();
        test.start();
    }
}
