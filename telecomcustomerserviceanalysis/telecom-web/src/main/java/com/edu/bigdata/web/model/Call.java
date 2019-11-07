package com.edu.bigdata.web.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.io.Serializable;

@Entity
@Table(name = "tb_call")
public class Call implements Serializable {
    @Id
    @Column(name = "id_contact_date")
    private String idContactDate;
    @Column(name = "id_dimension_contact")
    private int idDimensionContact;
    @Column(name = "id_dimension_date")
    private int idDimensionDate;
    @Column(name = "call_sum")
    private int callSum;
    @Column(name = "call_duration_sum")
    private int callDurationSum;

    public Call(String idContactDate, int idDimensionContact, int idDimensionDate, int callSum, int callDurationSum) {
        this.idContactDate = idContactDate;
        this.idDimensionContact = idDimensionContact;
        this.idDimensionDate = idDimensionDate;
        this.callSum = callSum;
        this.callDurationSum = callDurationSum;
    }

    public Call() {
    }

    public String getIdContactDate() {
        return idContactDate;
    }

    public void setIdContactDate(String idContactDate) {
        this.idContactDate = idContactDate;
    }

    public int getIdDimensionContact() {
        return idDimensionContact;
    }

    public void setIdDimensionContact(int idDimensionContact) {
        this.idDimensionContact = idDimensionContact;
    }

    public int getIdDimensionDate() {
        return idDimensionDate;
    }

    public void setIdDimensionDate(int idDimensionDate) {
        this.idDimensionDate = idDimensionDate;
    }

    public int getCallSum() {
        return callSum;
    }

    public void setCallSum(int callSum) {
        this.callSum = callSum;
    }

    public int getCallDurationSum() {
        return callDurationSum;
    }

    public void setCallDurationSum(int callDurationSum) {
        this.callDurationSum = callDurationSum;
    }
}
