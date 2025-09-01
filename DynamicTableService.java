package com.example.demo.service;

import com.example.demo.dto.BulkUploadFormColumn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class DynamicTableService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Safely truncate and reload a table with validated data
     * This method implements multiple layers of SQL injection protection
     */
    public void truncateAndReloadTable(String tableName, List<Map<String, Object>> data, 
                                     List<BulkUploadFormColumn> columns, String createdBy) {
        
        // 1. Validate table name - only allow alphanumeric and underscores
        if (!isValidTableName(tableName)) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }

        // 2. Validate column names - only allow alphanumeric and underscores
        List<String> validatedColumns = columns.stream()
                .map(BulkUploadFormColumn::getColumnName)
                .filter(this::isValidColumnName)
                .collect(Collectors.toList());

        if (validatedColumns.size() != columns.size()) {
            throw new IllegalArgumentException("Invalid column names detected");
        }

        // 3. Add required columns that are not in the bulk upload form
        List<String> allColumns = new ArrayList<>(validatedColumns);
        
        // Get required columns from the actual table schema
        List<String> requiredColumns = getRequiredColumns(tableName);
        System.out.println("Raw required columns from schema: " + requiredColumns);
        
        // Add any required columns that are missing (normalize to lowercase to avoid duplicates)
        for (String requiredColumn : requiredColumns) {
            String normalizedRequiredColumn = requiredColumn.toLowerCase();
            System.out.println("Checking column: " + requiredColumn + " -> normalized: " + normalizedRequiredColumn);
            
            if (!validatedColumns.contains(normalizedRequiredColumn) && !allColumns.contains(normalizedRequiredColumn)) {
                allColumns.add(normalizedRequiredColumn);
                System.out.println("Added required column: " + normalizedRequiredColumn + " (from " + requiredColumn + ")");
            } else {
                System.out.println("Column " + normalizedRequiredColumn + " already exists, skipping");
            }
        }

        // 4. Build safe SQL with parameterized queries
        String truncateSql = "TRUNCATE TABLE " + tableName;
        String insertSql = buildSafeInsertSql(tableName, allColumns);
        
        System.out.println("Generated INSERT SQL: " + insertSql);
        System.out.println("Columns being inserted: " + allColumns);
        System.out.println("Total columns: " + allColumns.size());
        
        // Final validation - ensure no duplicate columns
        Set<String> uniqueColumns = new HashSet<>(allColumns);
        if (uniqueColumns.size() != allColumns.size()) {
            throw new IllegalArgumentException("Duplicate columns detected in INSERT statement: " + allColumns);
        }
        System.out.println("Column validation passed - no duplicates found");

        try {
            // 5. Execute truncate
            jdbcTemplate.execute(truncateSql);
            
            // 6. Execute batch insert with parameterized values including required fields
            LocalDateTime now = LocalDateTime.now();
            System.out.println("Audit values - createdBy: " + createdBy + ", createdDate: " + now);
            
            for (int i = 0; i < data.size(); i++) {
                Map<String, Object> row = data.get(i);
                List<Object> allValues = new ArrayList<>();
                
                // Add data values from bulk upload form
                for (String column : validatedColumns) {
                    allValues.add(row.get(column));
                }
                
                // Add values for required columns that are not in the bulk upload form
                for (String requiredColumn : requiredColumns) {
                    String normalizedRequiredColumn = requiredColumn.toLowerCase();
                    if (!validatedColumns.contains(normalizedRequiredColumn)) {
                        if ("created_by".equals(normalizedRequiredColumn)) {
                            allValues.add(createdBy);
                        } else if ("created_date".equals(normalizedRequiredColumn)) {
                            allValues.add(now);
                        } else {
                            // For other required columns, add null or default value
                            allValues.add(null);
                        }
                    }
                }
                
                System.out.println("Row " + (i + 1) + " values: " + allValues);
                jdbcTemplate.update(insertSql, allValues.toArray());
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Error updating table " + tableName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Build safe INSERT SQL statement
     * Uses parameterized queries to prevent SQL injection
     */
    private String buildSafeInsertSql(String tableName, List<String> columns) {
        String columnList = String.join(", ", columns);
        String placeholders = columns.stream()
                .map(col -> "?")
                .collect(Collectors.joining(", "));
        
        return "INSERT INTO " + tableName + " (" + columnList + ") VALUES (" + placeholders + ")";
    }

    /**
     * Validate table name to prevent SQL injection
     * Only allows alphanumeric characters and underscores
     */
    private boolean isValidTableName(String tableName) {
        if (!StringUtils.hasText(tableName)) {
            return false;
        }
        // Only allow alphanumeric and underscores, must start with letter
        return tableName.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }

    /**
     * Validate column name to prevent SQL injection
     * Only allows alphanumeric characters and underscores
     */
    private boolean isValidColumnName(String columnName) {
        if (!StringUtils.hasText(columnName)) {
            return false;
        }
        // Only allow alphanumeric and underscores, must start with letter
        return columnName.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }

    /**
     * Get table metadata safely
     */
    public boolean tableExists(String tableName) {
        if (!isValidTableName(tableName)) {
            return false;
        }
        
        try {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tableName);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get required columns for a table (including NOT NULL columns)
     */
    public List<String> getRequiredColumns(String tableName) {
        if (!isValidTableName(tableName)) {
            return new ArrayList<>();
        }
        
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND IS_NULLABLE = 'NO'";
            List<String> requiredColumns = jdbcTemplate.queryForList(sql, String.class, tableName);
            System.out.println("Required columns for table " + tableName + ": " + requiredColumns);
            return requiredColumns;
        } catch (Exception e) {
            System.err.println("Error getting required columns: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Get access to JdbcTemplate for advanced queries
     * Used for uniqueness validation against existing data
     */
    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
} 