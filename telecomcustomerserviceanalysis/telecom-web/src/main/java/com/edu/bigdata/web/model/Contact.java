package com.edu.bigdata.web.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "tb_dimension_contacts")
public class Contact implements Serializable {
    @Id
    @Column(name = "id")
    private int id;
    @Column(name = "telephone")
    private String telephone;
    @Column(name = "name")
    private String name;

    public Contact() {
    }

    public Contact(int id, String telephone, String name) {
        this.id = id;
        this.telephone = telephone;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTelephone() {
        return telephone;
    }

    public void setTelephone(String telephone) {
        this.telephone = telephone;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Contact{" +
                "id=" + id +
                ", telephone='" + telephone + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
