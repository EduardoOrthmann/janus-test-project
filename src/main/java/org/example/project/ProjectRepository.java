package org.example.project;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ProjectRepository {
    private final Connection connection;

    public ProjectRepository(Connection connection) {
        this.connection = connection;
    }

    public void addProject(String name, int departmentId) throws SQLException {
        String sql = "INSERT INTO Projects (name, department_id) VALUES (?, ?)";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, name);
            stmt.setInt(2, departmentId);
            stmt.executeUpdate();
        }
    }

    public List<Project> getAllProjects() throws SQLException {
        List<Project> projects = new ArrayList<>();
        String sql = "SELECT * FROM Projects";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                projects.add(new Project(
                        rs.getInt("id"),
                        rs.getString("name"),
                        rs.getInt("department_id")
                ));
            }
        }
        return projects;
    }
}