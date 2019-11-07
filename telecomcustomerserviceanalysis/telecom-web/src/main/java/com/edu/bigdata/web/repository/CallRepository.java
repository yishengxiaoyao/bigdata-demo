package com.edu.bigdata.web.repository;

import com.edu.bigdata.web.model.Call;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CallRepository extends JpaRepository<Call,String> {
}
