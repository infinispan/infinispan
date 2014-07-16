package org.infinispan.statetransfer;

import java.io.Serializable;

public class BigObject implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;

    private String value;

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

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      BigObject bigObject = (BigObject) o;

      if (name != null ? !name.equals(bigObject.name) : bigObject.name != null) return false;
      if (value != null ? !value.equals(bigObject.value) : bigObject.value != null) return false;
      return true;
   }

   public int hashCode() {
        int result = getName() != null ? getName().hashCode() : 0;
        return result;
    }
}
