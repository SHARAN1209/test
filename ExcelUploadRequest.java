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
