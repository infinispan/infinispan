/* 
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
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
package org.infinispan.query.analysis;

import org.apache.solr.analysis.ASCIIFoldingFilterFactory;
import org.apache.solr.analysis.HTMLStripCharFilterFactory;
import org.apache.solr.analysis.LengthFilterFactory;
import org.apache.solr.analysis.LowerCaseFilterFactory;
import org.apache.solr.analysis.MappingCharFilterFactory;
import org.apache.solr.analysis.PatternTokenizerFactory;
import org.apache.solr.analysis.PorterStemFilterFactory;
import org.apache.solr.analysis.ShingleFilterFactory;
import org.apache.solr.analysis.SnowballPorterFilterFactory;
import org.apache.solr.analysis.StandardFilterFactory;
import org.apache.solr.analysis.StandardTokenizerFactory;
import org.apache.solr.analysis.StopFilterFactory;
import org.apache.solr.analysis.SynonymFilterFactory;
import org.apache.solr.analysis.WordDelimiterFilterFactory;
import org.hibernate.search.annotations.Analyzer;
import org.hibernate.search.annotations.AnalyzerDef;
import org.hibernate.search.annotations.AnalyzerDefs;
import org.hibernate.search.annotations.CharFilterDef;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Parameter;
import org.hibernate.search.annotations.TokenFilterDef;
import org.hibernate.search.annotations.TokenizerDef;

/**
 * @author Emmanuel Bernard
 */
@Indexed
@AnalyzerDefs({
      @AnalyzerDef(name = "customanalyzer",
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = ASCIIFoldingFilterFactory.class),
                  @TokenFilterDef(factory = LowerCaseFilterFactory.class),
                  @TokenFilterDef(factory = StopFilterFactory.class, params = {
                        @Parameter(name = "words",
                              value = "analysis/stoplist.properties"),
                        @Parameter(name="resource_charset", value = "UTF-8"),
                        @Parameter(name = "ignoreCase", value = "true")
                  }),
                  @TokenFilterDef(factory = SnowballPorterFilterFactory.class, params = {
                        @Parameter(name = "language", value = "English")
                  })
            }),

      @AnalyzerDef(name = "pattern_analyzer",
            tokenizer = @TokenizerDef(factory = PatternTokenizerFactory.class, params = {
                  @Parameter(name = "pattern", value = ",")
            })),

      @AnalyzerDef(name = "standard_analyzer",
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = StandardFilterFactory.class)
            }),

      @AnalyzerDef(name = "html_standard_analyzer",
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            },
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = StandardFilterFactory.class)
            }),

      @AnalyzerDef(name = "html_whitespace_analyzer",
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            }),

      @AnalyzerDef(name = "length_analyzer",
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = LengthFilterFactory.class, params = {
                        @Parameter(name = "min", value = "3"),
                        @Parameter(name = "max", value = "5")
                  })
            }),

      @AnalyzerDef(name = "porter_analyzer",
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = PorterStemFilterFactory.class)
            }),

      @AnalyzerDef(name = "word_analyzer",
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            },
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = WordDelimiterFilterFactory.class, params = {
                        @Parameter(name = "splitOnCaseChange", value = "1")
                  })
            }),

      @AnalyzerDef(name = "synonym_analyzer",
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            },
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = SynonymFilterFactory.class, params = {
                        @Parameter(name = "synonyms",
                              value = "analysis/synonyms.properties")
                  })
            }),

      @AnalyzerDef(name = "shingle_analyzer",
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            },
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class),
            filters = {
                  @TokenFilterDef(factory = ShingleFilterFactory.class)
            }),

      @AnalyzerDef(name = "html_char_analyzer",
            charFilters = {
                  @CharFilterDef(factory = HTMLStripCharFilterFactory.class)
            },
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class)
            ),

      @AnalyzerDef(name = "mapping_char_analyzer",
            charFilters = {
                  @CharFilterDef(factory = MappingCharFilterFactory.class, params = {
                        @Parameter(name = "mapping", value = "analysis/mapping-chars.properties")
                  })
            },    
            tokenizer = @TokenizerDef(factory = StandardTokenizerFactory.class)
      )
})
public class Team {

   @Field
   private String name;

   @Field
   private String location;

   @Field
   @Analyzer(definition = "customanalyzer")
   private String description;

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getLocation() {
      return location;
   }

   public void setLocation(String location) {
      this.location = location;
   }

   public String getDescription() {
      return description;
   }

   public void setDescription(String description) {
      this.description = description;
   }
}