package org.infinispan.it.osgi.query;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerSuite;

import static org.infinispan.it.osgi.util.IspnKarafOptions.perSuiteOptions;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * @author gustavonalle
 * @since 7.0
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerSuite.class)
public class SolrAnalyzerTest extends org.infinispan.query.analysis.SolrAnalyzerTest {

   @Before
   @Override
   public void setup() throws Exception {
      super.setup();
   }


   @Configuration
   public Option[] config() throws Exception {
      return options(perSuiteOptions());
   }

   @Test
   public void testAnalyzerDef() throws Exception {
      super.testAnalyzerDef(); 
   }

   @Test
   public void testAnalyzers() throws Exception {
      super.testAnalyzers();
   }
}
