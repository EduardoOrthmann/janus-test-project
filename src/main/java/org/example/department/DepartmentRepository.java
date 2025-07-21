package org.example.department;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DepartmentRepository {
    private final Connection connection;

    public DepartmentRepository(Connection connection) {
        this.connection = connection;
    }

    public void addDepartment(String name, String location) throws SQLException {
        String sql = "INSERT INTO Departments (name, location) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setString(2, location);
            stmt.executeUpdate();
        }
    }

    public Department getDepartmentById(int id) throws SQLException {
        String sql = "SELECT * FROM Departments WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, id);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return new Department(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("location")
                );
            } else {
                return null;
            }
        }
    }

    public List<Department> getAllDepartments() throws SQLException {
        List<Department> departments = new ArrayList<>();
        String sql = "SELECT * FROM Departments";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                departments.add(new Department(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getString("location")
                ));
            }
        }
        return departments;
    }

    public void updateDepartment(int id, String name, String location) throws SQLException {
        String sql = "UPDATE Departments SET name = ?, location = ? WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setString(2, location);
            stmt.setInt(3, id);
            stmt.executeUpdate();
        }
    }

    public void deleteDepartment(int id) throws SQLException {
        String sql = "DELETE FROM Departments WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, id);
            stmt.executeUpdate();
        }
    }

    public void listDepartmentsWithProjects() throws SQLException {
        String sql = "SELECT d.name AS dept_name, p.name AS project_name " +
                     "FROM Departments d " +
                     "LEFT OUTER JOIN Projects p ON d.id = p.department_id " +
                     "FETCH FIRST 10 ROWS ONLY";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("Department: %s | Project: %s%n",
                        rs.getString("dept_name"),
                        rs.getString("project_name") != null ? rs.getString("project_name") : "(No project)");
            }
        }
    }
}