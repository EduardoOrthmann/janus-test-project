package org.example;


import org.example.department.Department;
import org.example.department.DepartmentRepository;
import org.example.employee.EmployeeRepository;
import org.example.project.ProjectRepository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        String jdbcUrl = "jdbc:db2://localhost:50000/test-db2";
        String username = "password";
        String password = "password";

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            EmployeeRepository employeeRepo = new EmployeeRepository(conn);
            DepartmentRepository departmentRepo = new DepartmentRepository(conn);
            ProjectRepository projectRepo = new ProjectRepository(conn);

            // Create departments
            departmentRepo.addDepartment("Engineering", "São Paulo");
            departmentRepo.addDepartment("HR", "Rio de Janeiro");

            // Get department ID
            List<Department> departments = departmentRepo.getAllDepartments();
            int engineeringId = departments.getFirst().getId();

            // Add employees
            employeeRepo.addEmployee("Alice", "Smith", engineeringId, 75000);
            employeeRepo.addEmployee("Bob", "Johnson", engineeringId, 60000);

            // Add projects
            projectRepo.addProject("AI Research", engineeringId);
            projectRepo.addProject("Migration", engineeringId);

            // List employees
            System.out.println("\n--- All Employees ---");
            employeeRepo.getAllEmployees().forEach(System.out::println);

            // List projects
            System.out.println("\n--- All Projects ---");
            projectRepo.getAllProjects().forEach(System.out::println);

            // List departments with LEFT OUTER JOIN
            System.out.println("\n--- Departments and Projects ---");
            departmentRepo.listDepartmentsWithProjects();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}