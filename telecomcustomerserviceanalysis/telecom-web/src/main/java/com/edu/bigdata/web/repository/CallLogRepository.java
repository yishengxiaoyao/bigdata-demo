package com.edu.bigdata.web.repository;

import com.edu.bigdata.web.model.CallLog;
import com.edu.bigdata.web.model.Contact;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface CallLogRepository extends JpaRepository<Contact, Integer>{
    @Query(value = "SELECT name, telephone, call_sum, call_duration_sum, year, month, day \n" +
            "FROM tb_dimension_date t4 \n" +
            "INNER JOIN (SELECT id_dimension_date, call_sum, call_duration_sum, telephone, name \n" +
            "FROM tb_call t2 \n" +
            "INNER JOIN (\n" +
            "SELECT id, telephone, name \n" +
            "FROM tb_dimension_contacts \n" +
            "WHERE telephone = :telephone ) t1 ON \n" +
            "t2.id_dimension_contact = t1.id ) t3 ON \n" +
            "t4.id = t3.id_dimension_date \n" +
            "WHERE (year = :year AND month != :month AND day = :day) \n" +
            "ORDER BY year, month;",nativeQuery = true)
    List<CallLog> getCallLogList(@Param("telephone")String telephone, @Param("year")int year, @Param("month")int month, @Param("day")int day);
}
