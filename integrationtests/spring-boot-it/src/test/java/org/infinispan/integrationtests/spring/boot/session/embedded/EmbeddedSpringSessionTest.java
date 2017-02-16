package org.infinispan.integrationtests.spring.boot.session.embedded;

import org.infinispan.integrationtests.spring.boot.session.AbstractSpringSessionTCK;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EmbeddedConfiguration.class, webEnvironment= SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EmbeddedSpringSessionTest extends AbstractSpringSessionTCK {

}
