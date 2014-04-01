package org.infinispan.persistence.jpa.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Entity
public class Huge {
   @Id
   public String id;
   @Lob
   @Column(length = 1 << 20)
   public byte[] data;

   public Huge() {
   }

   public Huge(String id, byte[] data) {
      this.id = id;
      this.data = data;
   }
}
