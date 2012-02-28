/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.statetransfer;

import java.io.Serializable;

public class BigObject implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

     
    private String value;

     
    private String value2;

     
    private String value3;

     
    private String value4;

     
    private String value5;

     
    private String value6;

     
    private String value7;

     
    private String value8;

     
    private String value9;

     
    private String value10;

     
    private String value11;

     
    private String value12;

     
    private String value13;

     
    private String value14;

     
    private String value15;

     
    private String value16;

     
    private String value17;

     
    private String value18;

     
    private String value19;

     
    private String value20;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }

    public String getValue3() {
        return value3;
    }

    public void setValue3(String value3) {
        this.value3 = value3;
    }

    public void setValue4(String value4) {
        this.value4 = value4;
    }

    public String getValue4() {
        return value4;
    }

    public void setValue5(String value5) {
        this.value5 = value5;
    }

    public String getValue5() {
        return value5;
    }

    public void setValue6(String value6) {
        this.value6 = value6;
    }

    public String getValue6() {
        return value6;
    }

    public void setValue7(String value7) {
        this.value7 = value7;
    }

    public String getValue7() {
        return value7;
    }

    public void setValue8(String value8) {
        this.value8 = value8;
    }

    public String getValue8() {
        return value8;
    }

    public void setValue10(String value10) {
        this.value10 = value10;
    }

    public String getValue10() {
        return value10;
    }

    public void setValue9(String value9) {
        this.value9 = value9;
    }

    public String getValue9() {
        return value9;
    }

    public void setValue11(String value11) {
        this.value11 = value11;
    }

    public String getValue11() {
        return value11;
    }

    public void setValue12(String value12) {
        this.value12 = value12;
    }

    public String getValue12() {
        return value12;
    }

    public void setValue13(String value13) {
        this.value13 = value13;
    }

    public String getValue13() {
        return value13;
    }

    public void setValue14(String value14) {
        this.value14 = value14;
    }

    public String getValue14() {
        return value14;
    }

    public void setValue15(String value15) {
        this.value15 = value15;
    }

    public String getValue15() {
        return value15;
    }

    public void setValue16(String value16) {
        this.value16 = value16;
    }

    public String getValue16() {
        return value16;
    }

    public void setValue17(String value17) {
        this.value17 = value17;
    }

    public String getValue17() {
        return value17;
    }

    public void setValue18(String value18) {
        this.value18 = value18;
    }

    public String getValue18() {
        return value18;
    }

    public void setValue19(String value19) {
        this.value19 = value19;
    }

    public String getValue19() {
        return value19;
    }

    public void setValue20(String value20) {
        this.value20 = value20;
    }

    public String getValue20() {
        return value20;
    }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BigObject bigObject = (BigObject) o;

      if (name != null ? !name.equals(bigObject.name) : bigObject.name != null) return false;
      if (value != null ? !value.equals(bigObject.value) : bigObject.value != null) return false;
      if (value10 != null ? !value10.equals(bigObject.value10) : bigObject.value10 != null) return false;
      if (value11 != null ? !value11.equals(bigObject.value11) : bigObject.value11 != null) return false;
      if (value12 != null ? !value12.equals(bigObject.value12) : bigObject.value12 != null) return false;
      if (value13 != null ? !value13.equals(bigObject.value13) : bigObject.value13 != null) return false;
      if (value14 != null ? !value14.equals(bigObject.value14) : bigObject.value14 != null) return false;
      if (value15 != null ? !value15.equals(bigObject.value15) : bigObject.value15 != null) return false;
      if (value16 != null ? !value16.equals(bigObject.value16) : bigObject.value16 != null) return false;
      if (value17 != null ? !value17.equals(bigObject.value17) : bigObject.value17 != null) return false;
      if (value18 != null ? !value18.equals(bigObject.value18) : bigObject.value18 != null) return false;
      if (value19 != null ? !value19.equals(bigObject.value19) : bigObject.value19 != null) return false;
      if (value2 != null ? !value2.equals(bigObject.value2) : bigObject.value2 != null) return false;
      if (value20 != null ? !value20.equals(bigObject.value20) : bigObject.value20 != null) return false;
      if (value3 != null ? !value3.equals(bigObject.value3) : bigObject.value3 != null) return false;
      if (value4 != null ? !value4.equals(bigObject.value4) : bigObject.value4 != null) return false;
      if (value5 != null ? !value5.equals(bigObject.value5) : bigObject.value5 != null) return false;
      if (value6 != null ? !value6.equals(bigObject.value6) : bigObject.value6 != null) return false;
      if (value7 != null ? !value7.equals(bigObject.value7) : bigObject.value7 != null) return false;
      if (value8 != null ? !value8.equals(bigObject.value8) : bigObject.value8 != null) return false;
      if (value9 != null ? !value9.equals(bigObject.value9) : bigObject.value9 != null) return false;

      return true;
   }

   public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        return result;
    }
}
