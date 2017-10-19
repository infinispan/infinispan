package org.infinispan.counter.impl.externalizers;

/**
 * Ids range: 2000 - 2050
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface ExternalizerIds {

   //2000 CounterConfiguration in commons
   //2001 CounterState in commons
   Integer RESET_FUNCTION = 2002;
   Integer CONVERTER_AND_FILTER = 2003;
   Integer STRONG_COUNTER_KEY = 2004;
   Integer WEAK_COUNTER_KEY = 2005;
   Integer READ_FUNCTION = 2006;
   Integer COUNTER_VALUE = 2007;
   Integer COUNTER_METADATA = 2008;
   Integer INITIALIZE_FUNCTION = 2009;
   Integer ADD_FUNCTION = 2010;
   Integer CAS_FUNCTION = 2011;
   Integer CREATE_CAS_FUNCTION = 2012;
   Integer CREATE_ADD_FUNCTION = 2013;
   Integer REMOVE_FUNCTION = 2014;
}
