package org.example.employee;

public class Employee {
    private int id;
    private String firstName;
    private String lastName;
    private int departmentId;
    private double salary;

    public Employee(int id, String firstName, String lastName, int departmentId, double salary) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.departmentId = departmentId;
        this.salary = salary;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public int getDepartment() {
        return departmentId;
    }

    public void setDepartment(int department) {
        this.departmentId = department;
    }

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    @Override
    public String toString() {
        return "Employee {\n" +
               "        \tid = " + id + "\n" +
               "        \tfirstName = '" + firstName + '\'' + "\n" +
               "        \tlastName = '" + lastName + '\'' + "\n" +
               "        \tdepartmentId = " + departmentId + "\n" +
               "        \tsalary = " + salary + "\n" +
               '}';
    }
}