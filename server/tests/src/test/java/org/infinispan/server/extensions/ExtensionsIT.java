package org.infinispan.server.extensions;

import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerRuleBuilder;
import org.infinispan.tasks.ServerTask;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
@RunWith(Suite.class)
@Suite.SuiteClasses({
      ScriptingTasks.class,
      ServerTasks.class,
      PojoMarshalling.class
})
public class ExtensionsIT {
   @ClassRule
   public static final InfinispanServerRule SERVERS =
         InfinispanServerRuleBuilder.config("configuration/ClusteredServerTest.xml")
               .runMode(ServerRunMode.CONTAINER)
               .numServers(2)
               .artifacts(artifacts())
               .build();

   public static JavaArchive[] artifacts() {
      JavaArchive hello = ShrinkWrap.create(JavaArchive.class, "hello-server-task.jar");
      hello.addClass(HelloServerTask.class);
      hello.addAsServiceProvider(ServerTask.class, HelloServerTask.class);

      JavaArchive distHello = ShrinkWrap.create(JavaArchive.class, "distributed-hello-server-task.jar");
      distHello.addPackage(DistributedHelloServerTask.class.getPackage());
      distHello.addAsServiceProvider(ServerTask.class, DistributedHelloServerTask.class);

      JavaArchive pojo = ShrinkWrap.create(JavaArchive.class, "pojo.jar");
      pojo.addClass(Person.class);

      return new JavaArchive[]{hello, distHello, pojo};
   }
}
