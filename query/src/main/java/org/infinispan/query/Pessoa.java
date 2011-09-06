package org.infinispan.query;

import java.io.Serializable;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;

@Indexed
@ProvidedId
public class Pessoa implements Serializable {

   private static final long serialVersionUID = 6416216469201441157L;

   @Field(index = Index.TOKENIZED, store = Store.YES)
   private String nome;

   @Field(index = Index.TOKENIZED, store = Store.YES)
   private String sobrenome;

   @Field
   private Integer idade;

   @Field(index = Index.UN_TOKENIZED, store = Store.YES)
   private String sortBy;

   public String getNome() {
      return nome;
   }

   public void setNome(String nome) {
      this.nome = nome;
   }

   public int getIdade() {
      return idade;
   }

   public void setIdade(int idade) {
      this.idade = idade;
   }

   public void setSortBy(String sortBy) {
      this.sortBy = sortBy;
   }

   public String getSortBy() {
      return sortBy;
   }

   public void setSobrenome(String sobrenome) {
      this.sobrenome = sobrenome;
   }

   public String getSobrenome() {
      return sobrenome;
   }

}
