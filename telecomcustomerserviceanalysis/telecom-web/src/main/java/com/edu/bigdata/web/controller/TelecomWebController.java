package com.edu.bigdata.web.controller;

import com.edu.bigdata.web.model.CallLog;
import com.edu.bigdata.web.model.Contact;
import com.edu.bigdata.web.repository.CallLogRepository;
import com.edu.bigdata.web.repository.ContactRepository;
import com.edu.bigdata.web.util.GsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping(value = {"/v1/tele"})
public class TelecomWebController {
    @Autowired
    private ContactRepository contactRepository;
    @Autowired
    private CallLogRepository callLogRepository;
    @GetMapping(value = "/contact/{id}", produces = "application/json")
    public ResponseEntity<String> getContactById(@PathVariable("id")int id){
        List<Contact> contacts=contactRepository.getContactWithId(id);
        return GsonUtil.getResult(contacts, HttpStatus.OK);
    }

    @GetMapping(value = {"/contact/all"},produces = "application/json")
    public ResponseEntity<String> getAllContact(){
        List<Contact> contacts=contactRepository.getContacts();
        return GsonUtil.getResult(contacts,HttpStatus.OK);
    }

    @GetMapping(value = "/query/calllog",produces = "application/json")
    public ResponseEntity<String> getCallLogBy(@RequestParam("telephone")String telephone,
                                               @RequestParam("year")int year,
                                               @RequestParam("month")int month,
                                               @RequestParam("day")int day){
        List<CallLog> callLogs=callLogRepository.getCallLogList(telephone,year,month,day);
        return GsonUtil.getResult(callLogs,HttpStatus.OK);
    }

}
