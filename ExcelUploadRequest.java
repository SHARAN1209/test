package com.example.demo.dto;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;

@Data
public class ExcelUploadRequest {
    private List<String> groups;
    private MultipartFile file;

} 

@PostMapping("/{table_id}/versions")
    public ResponseEntity<Map<String, Object>> uploadExcelFile(
            @PathVariable("table_id") String tableId,
            @RequestParam("groups") List<String> groups,
            @RequestParam("file") MultipartFile file) {
        
        try {
            Map<String, Object> result = excelUploadService.uploadExcelFile(tableId, groups, file);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", e.getMessage()));
        }
    }

curl --location 'http://localhost:8080/tables/TABLE2/versions' \
--form 'file=@"/C:/Users/2127859/Downloads/Book 22.xlsx"' \
--form 'groups="admin"' \
--form 'user="admin.user"'

-- Sample data for LookUpTableEntity
INSERT INTO ruleapp_table_information (table_name, key_columns, notes, downloadable_by_groups, uploadable_by_groups, bulk_upload_form, lookup_description, lookup_categories) 
VALUES (
    'TABLE2',
    '["code"]',
    'Sample table for testing Excel upload',
    '["admin", "viewer"]',
    '["admin", "uploader"]',
    '[
    {
        "excel_column_name": "Code",
        "column_name": "code",
        "regex_pattern": "^[A-Z]{3}[0-9]{3}$",
        "cast_to": "string"
    },
    {
        "excel_column_name": "Description",
        "column_name": "description",
        "regex_pattern": "^[A-Za-z0-9 ]+$",
        "cast_to": "string"
    }
]',
    'Sample table for testing purposes',
    'test, sample'
);
# File Upload Configuration
spring.servlet.multipart.max-file-size=10MB
spring.servlet.multipart.max-request-size=10MB

# Database Initialization
spring.jpa.hibernate.ddl-auto=none
spring.jpa.defer-datasource-initialization=true
spring.sql.init.mode=always

package com.example.demo.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableJpaRepositories(basePackages = "com.example.demo.repository")
@EnableTransactionManagement
public class DatabaseConfig {
    // Configuration for database initialization
} 

