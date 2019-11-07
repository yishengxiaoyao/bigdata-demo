package com.edu.bigdata.web.repository;

import com.edu.bigdata.web.model.Contact;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface ContactRepository extends JpaRepository<Contact,Integer> {

    @Query(value = "SELECT id, telephone, name FROM tb_dimension_contacts",nativeQuery = true)
    List<Contact> getContacts();

    @Query(value = "SELECT id, telephone, name FROM tb_dimension_contacts WHERE id = :id",nativeQuery = true)
    List<Contact> getContactWithId(@Param("id")int id);
}
