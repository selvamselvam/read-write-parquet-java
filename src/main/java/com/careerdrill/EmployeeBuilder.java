package com.careerdrill;

import com.careerdrill.entity.Employee;
import com.careerdrill.entity.mailing_address;

public  class EmployeeBuilder {
    public static Employee buildEmployee(){
        Employee employee= new Employee();
        employee.setUserId(11);
        employee.setAge(10);
        employee.setFirstName("Bill");
        employee.setLastName("Gates");

        mailing_address address = new mailing_address();
        address.setCity("Bangalore");
        address.setStreet("Niladri");
        address.setStateProv("Karnataka");
        address.setZip("560001");
        address.setCountry("india");
        employee.setAddress(address);
        return employee;
    }
}
