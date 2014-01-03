package org.infinispan.server.test.cs.custom;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.infinispan.arquillian.core.InfinispanResource;
import org.infinispan.arquillian.core.RemoteInfinispanServer;
import org.infinispan.arquillian.core.WithRunningServer;
import org.infinispan.arquillian.utils.MBeanServerConnectionProvider;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.server.test.category.CacheStore;
import org.infinispan.server.test.util.TestUtil;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests custom cache store configuration. Store classes are copied into infinispan-core jar before the server is started.
 *
 * @author <a href="mailto:jmarkos@redhat.com">Jakub Markos</a>
 *
 */
@RunWith(Arquillian.class)
@Category(CacheStore.class)
public class CustomCacheStoreTest {

    @InfinispanResource("standalone-customcs")
    RemoteInfinispanServer server;

    final int managementPort = 9999;
    final String cacheLoaderMBean = "jboss.infinispan:type=Cache,name=\"default(local)\",manager=\"local\",component=CacheLoader";

    @BeforeClass
    public static void before() {
        // need to put the brno loader classes into ispn core jar, so ispn can see them
        String serverDir = System.getProperty("server1.dist");
        File ispnCoreJarDir = new File(serverDir + "/modules/system/layers/base/org/infinispan/main/");
        FileFilter ispnCoreJarFilter = new WildcardFileFilter("infinispan-core*.jar");
        File ispnCoreJar = ispnCoreJarDir.listFiles(ispnCoreJarFilter)[0];

        String customLoaderClassesDir = System.getProperty("basedir") + "/target/test-classes/org/infinispan/persistence/cluster/";
        File[] csClasses = {new File(customLoaderClassesDir + "MyCustomCacheStore.class"),
                            new File(customLoaderClassesDir + "MyCustomCacheStoreConfiguration.class"),
                            new File(customLoaderClassesDir + "MyCustomCacheStoreConfigurationBuilder.class")};
        addFilesToZip(ispnCoreJar, csClasses, "org/infinispan/persistence/cluster/");
    }

    @Test
    @WithRunningServer("standalone-customcs")
    public void test() throws Exception {
        RemoteCacheManager rcm = TestUtil.createCacheManager(server);
        RemoteCache<String, String> rc = rcm.getCache();
        assertNull(rc.get("key1"));
        rc.put("key1", "value1");
        assertEquals("value1", rc.get("key1"));
        // check via jmx that MyCustomCacheStore is indeed used
        MBeanServerConnectionProvider provider = new MBeanServerConnectionProvider(server.getHotrodEndpoint().getInetAddress().getHostName(), managementPort);
        assertEquals("[org.infinispan.persistence.cluster.MyCustomCacheStore]", getAttribute(provider, cacheLoaderMBean, "stores"));
    }

    private String getAttribute(MBeanServerConnectionProvider provider, String mbean, String attr) throws Exception {
        return provider.getConnection().getAttribute(new ObjectName(mbean), attr).toString();
    }

    /*
    * Credit goes to user577732 - http://stackoverflow.com/a/9305091/2064880 (slightly modified)
    */
    public static void addFilesToZip(File source, File[] files, String path){
        try{
            File tmpZip = new File(source.getAbsolutePath()+".tmp");
            FileUtils.copyFile(source, tmpZip);

            byte[] buffer = new byte[4096];
            ZipInputStream zin = new ZipInputStream(new FileInputStream(tmpZip));
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(source));
            for(int i = 0; i < files.length; i++){
                InputStream in = new FileInputStream(files[i]);
                out.putNextEntry(new ZipEntry(path + files[i].getName()));
                for(int read = in.read(buffer); read > -1; read = in.read(buffer)){
                    out.write(buffer, 0, read);
                }
                out.closeEntry();
                in.close();
            }
            for(ZipEntry ze = zin.getNextEntry(); ze != null; ze = zin.getNextEntry()){
                if(!zipEntryMatch(ze.getName(), files, path)){
                    out.putNextEntry(ze);
                    for(int read = zin.read(buffer); read > -1; read = zin.read(buffer)){
                        out.write(buffer, 0, read);
                    }
                    out.closeEntry();
                }
            }
            out.close();
            tmpZip.delete();
        }catch(Exception e){
            e.printStackTrace();
        }
    }

   public static boolean zipEntryMatch(String zeName, File[] files, String path){
        for(int i = 0; i < files.length; i++){
            if((path + files[i].getName()).equals(zeName)){
                return true;
            }
        }
        return false;
    }
}
