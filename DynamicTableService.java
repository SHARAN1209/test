package com.example.demo.service;

import com.example.demo.dto.BulkUploadFormColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class DynamicTableService {

    private static final Logger log = LoggerFactory.getLogger(DynamicTableService.class);

    private final JdbcTemplate jdbcTemplate;

    public DynamicTableService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void truncateAndReloadTable(String tableName, List<Map<String, Object>> data,
                                       List<BulkUploadFormColumn> columns, String createdBy) {

        validateTableAndColumns(tableName, columns);
        List<String> allColumns = resolveAllColumns(tableName, columns);
        String insertSql = buildSafeInsertSql(tableName, allColumns);

        jdbcTemplate.execute("TRUNCATE TABLE " + tableName);

        LocalDateTime now = LocalDateTime.now();
        List<Object[]> batchArgs = data.stream()
                .map(row -> mapRowValues(row, allColumns, createdBy, now))
                .toList();

        jdbcTemplate.batchUpdate(insertSql, batchArgs);

        log.info("Inserted {} rows into {}", data.size(), tableName);
    }

    private void validateTableAndColumns(String tableName, List<BulkUploadFormColumn> columns) {
        if (!isValidTableName(tableName)) throw new IllegalArgumentException("Invalid table name: " + tableName);

        List<String> validatedColumns = columns.stream()
                .map(BulkUploadFormColumn::getColumnName)
                .filter(this::isValidColumnName)
                .toList();

        if (validatedColumns.size() != columns.size()) {
            throw new IllegalArgumentException("Invalid column names detected in bulk upload form");
        }
    }

    private List<String> resolveAllColumns(String tableName, List<BulkUploadFormColumn> columns) {
        List<String> validated = columns.stream().map(BulkUploadFormColumn::getColumnName).map(String::toLowerCase).toList();
        List<String> required = getRequiredColumns(tableName).stream().map(String::toLowerCase).toList();

        Set<String> all = new LinkedHashSet<>(validated);
        all.addAll(required);
        return new ArrayList<>(all);
    }

    private Object[] mapRowValues(Map<String, Object> row, List<String> allColumns, String createdBy, LocalDateTime now) {
        return allColumns.stream()
                .map(col -> switch (col) {
                    case "created_by" -> createdBy;
                    case "created_date" -> now;
                    default -> row.getOrDefault(col, null);
                })
                .toArray();
    }

    private String buildSafeInsertSql(String tableName, List<String> columns) {
        String colList = String.join(", ", columns);
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));
        return String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, colList, placeholders);
    }

    private boolean isValidTableName(String name) {
        return StringUtils.hasText(name) && name.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }

    private boolean isValidColumnName(String name) {
        return StringUtils.hasText(name) && name.matches("^[a-zA-Z][a-zA-Z0-9_]*$");
    }

    public List<String> getRequiredColumns(String tableName) {
        if (!isValidTableName(tableName)) return List.of();
        try {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND IS_NULLABLE = 'NO'";
            return jdbcTemplate.queryForList(sql, String.class, tableName);
        } catch (Exception e) {
            log.error("Error fetching required columns for {}: {}", tableName, e.getMessage());
            return List.of();
        }
    }

    public boolean tableExists(String tableName) {
        if (!isValidTableName(tableName)) return false;
        try {
            String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
            Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tableName);
            return count != null && count > 0;
        } catch (Exception e) {
            return false;
        }
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }
}
