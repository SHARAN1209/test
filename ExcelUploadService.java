package com.example.demo.service;

import com.example.demo.dto.BulkUploadFormColumn;
import com.example.demo.dto.ValidationError;
import com.example.demo.entity.LookUpTableEntity;
import com.example.demo.entity.RuleAppTableVersionEntity;
import com.example.demo.entity.RuleappHistoricalDataEntity;
import com.example.demo.repository.LookUpTableRepository;
import com.example.demo.repository.RuleAppTableVersionRepository;
import com.example.demo.repository.RuleappHistoricalDataRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ExcelUploadService {

    @Autowired
    private LookUpTableRepository lookUpTableRepository;
    
    @Autowired
    private RuleAppTableVersionRepository ruleAppTableVersionRepository;
    
    @Autowired
    private RuleappHistoricalDataRepository ruleappHistoricalDataRepository;
    
    @Autowired
    private DynamicTableService dynamicTableService;

    private final ObjectMapper objectMapper;
    
    public ExcelUploadService() {
        this.objectMapper = new ObjectMapper();
        // Configure ObjectMapper to be more lenient
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        
        // Register JSR310 module to handle Java 8 date/time types
        this.objectMapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
        // Disable writing dates as timestamps to get ISO format
        this.objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public Map<String, Object> uploadExcelFile(String tableName, List<String> groups, MultipartFile file) {
        // 1. Find LookUpTableEntity by table name
        LookUpTableEntity lookUpTable = lookUpTableRepository.findByTableName(tableName)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Table not found: " + tableName));

        // 2. Check if user groups have upload permission
        if (!hasUploadPermission(lookUpTable.getUploadableByGroups(), groups)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User not authorized to upload to this table");
        }

        // 3. Parse bulkUploadForm and keyColumns
        System.out.println("Retrieved bulkUploadForm from DB: " + lookUpTable.getBulkUploadForm());
        System.out.println("Retrieved keyColumns from DB: " + lookUpTable.getKeyColumns());
        
        List<BulkUploadFormColumn> bulkUploadForm = parseBulkUploadForm(lookUpTable.getBulkUploadForm());
        List<String> keyColumns = parseKeyColumns(lookUpTable.getKeyColumns());

        // 4. Validate Excel file and extract data
        List<Map<String, Object>> excelData = validateAndExtractExcelData(file, bulkUploadForm, keyColumns);
        

        // 5. Add audit fields
        String currentUser = "currentUser"; // Replace with actual user from security context
        List<Map<String, Object>> finalData = addAuditFields(excelData, currentUser);

        // 6. Print the final data
        System.out.println("Final data to be inserted:");
        finalData.forEach(System.out::println);

        // 7. Update version management
        String versionString = updateTableVersion(tableName, currentUser);

        // 8. Truncate and reload destination table
        dynamicTableService.truncateAndReloadTable(tableName, finalData, bulkUploadForm, currentUser);

        // 9. Store historical data
        storeHistoricalData(tableName, finalData, versionString, currentUser);

        return Map.of(
                "message", "Excel file processed successfully",
                "totalRows", finalData.size(),
                "data", finalData
        );
    }

    private boolean hasUploadPermission(String uploadableByGroups, List<String> userGroups) {
        if (uploadableByGroups == null || userGroups == null) {
            return false;
        }
        
        try {
            List<String> allowedGroups = objectMapper.readValue(uploadableByGroups, new TypeReference<List<String>>() {});
            return userGroups.stream().anyMatch(allowedGroups::contains);
        } catch (Exception e) {
            return false;
        }
    }

    private List<BulkUploadFormColumn> parseBulkUploadForm(String bulkUploadFormJson) {
        try {
            if (bulkUploadFormJson == null || bulkUploadFormJson.trim().isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Bulk upload form is null or empty");
            }
            
            // Clean the JSON string - remove any extra whitespace and escape characters
            String cleanedJson = bulkUploadFormJson.trim();
            
            // Log the JSON for debugging
            System.out.println("Parsing bulk upload form JSON: " + cleanedJson);
            
            List<BulkUploadFormColumn> result = objectMapper.readValue(cleanedJson, new TypeReference<List<BulkUploadFormColumn>>() {});
            
            // Validate the parsed result
            if (result == null || result.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Bulk upload form is empty after parsing");
            }
            
            System.out.println("Successfully parsed " + result.size() + " columns");
            
            // Validate each column has required fields
            for (BulkUploadFormColumn column : result) {
                if (column.getExcelColumnName() == null || column.getColumnName() == null) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                        "Column missing required fields: excelColumnName or columnName");
                }
                System.out.println("Column: " + column.getExcelColumnName() + " -> " + column.getColumnName());
            }
            
            return result;
            
        } catch (Exception e) {
            System.err.println("Error parsing bulk upload form: " + e.getMessage());
            e.printStackTrace();
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                "Invalid bulk upload form format: " + e.getMessage() + ". JSON: " + bulkUploadFormJson);
        }
    }

    private List<String> parseKeyColumns(String keyColumnsJson) {
        try {
            if (keyColumnsJson == null || keyColumnsJson.trim().isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Key columns is null or empty");
            }
            
            // Clean the JSON string
            String cleanedJson = keyColumnsJson.trim();
            
            // Log the JSON for debugging
            System.out.println("Parsing key columns JSON: " + cleanedJson);
            
            List<String> result = objectMapper.readValue(cleanedJson, new TypeReference<List<String>>() {});
            
            // Validate the parsed result
            if (result == null || result.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Key columns is empty after parsing");
            }
            
            System.out.println("Successfully parsed " + result.size() + " key columns");
            return result;
            
        } catch (Exception e) {
            System.err.println("Error parsing key columns: " + e.getMessage());
            e.printStackTrace();
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                "Invalid key columns format: " + e.getMessage() + ". JSON: " + keyColumnsJson);
        }
    }

    private List<Map<String, Object>> validateAndExtractExcelData(MultipartFile file, 
                                                                 List<BulkUploadFormColumn> bulkUploadForm, 
                                                                 List<String> keyColumns) {
        List<Map<String, Object>> excelData = new ArrayList<>();
        List<ValidationError> validationErrors = new ArrayList<>();

        try (Workbook workbook = new XSSFWorkbook(file.getInputStream())) {
            Sheet sheet = workbook.getSheetAt(0);
            
            // Check if file has at least one row
            if (sheet.getPhysicalNumberOfRows() < 2) { // Header + at least one data row
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Excel file must contain at least one data row");
            }

            // Extract and validate header row
            Row headerRow = sheet.getRow(0);
            Map<String, Integer> columnIndexMap = new HashMap<>();
            Set<String> expectedColumns = bulkUploadForm.stream()
                    .map(BulkUploadFormColumn::getExcelColumnName)
                    .collect(Collectors.toSet());

            for (int i = 0; i < headerRow.getLastCellNum(); i++) {
                Cell cell = headerRow.getCell(i);
                if (cell != null) {
                    String columnName = cell.getStringCellValue();
                    columnIndexMap.put(columnName, i);
                }
            }

            // Validate that all expected columns are present
            if (!columnIndexMap.keySet().containsAll(expectedColumns)) {
                Set<String> missingColumns = new HashSet<>(expectedColumns);
                missingColumns.removeAll(columnIndexMap.keySet());
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                    "Missing required columns: " + String.join(", ", missingColumns));
            }

            // Process data rows
            Set<String> uniqueKeyCombinations = new HashSet<>();
            
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null) continue;

                Map<String, Object> rowData = new HashMap<>();
                String keyCombination = "";

                for (BulkUploadFormColumn formColumn : bulkUploadForm) {
                    String excelColumnName = formColumn.getExcelColumnName();
                    String dbColumnName = formColumn.getColumnName();
                    Integer columnIndex = columnIndexMap.get(excelColumnName);
                    
                    if (columnIndex == null) continue;

                    Cell cell = row.getCell(columnIndex);
                    String cellValue = getCellValueAsString(cell);
                    
                    // Handle null/empty values
                    if (cellValue == null || cellValue.trim().isEmpty()) {
                        cellValue = ""; // Normalize null to empty string
                    }
                    
                    // Validate regex pattern (skip validation for empty values unless pattern specifically requires content)
                    if (formColumn.getRegexPattern() != null && !formColumn.getRegexPattern().isEmpty()) {
                        if (!cellValue.isEmpty() || formColumn.getRegexPattern().contains("+") || formColumn.getRegexPattern().contains("*")) {
                            if (!Pattern.matches(formColumn.getRegexPattern(), cellValue)) {
                                validationErrors.add(new ValidationError(
                                    rowIndex + 1, columnIndex + 1, excelColumnName, cellValue,
                                    "Value does not match regex pattern: " + formColumn.getRegexPattern()
                                ));
                            }
                        }
                    }

                    // Cast value if specified
                    Object castedValue = castValue(cellValue, formColumn.getCastTo());
                    rowData.put(dbColumnName, castedValue);

                    // Build key combination for uniqueness check
                    if (keyColumns.contains(dbColumnName)) {
                        keyCombination += (keyCombination.isEmpty() ? "" : "|") + cellValue;
                    }
                }

                // Validate key uniqueness
                if (!keyCombination.isEmpty()) {
                    if (uniqueKeyCombinations.contains(keyCombination)) {
                        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                            "Duplicate key combination found at row " + (rowIndex + 1) + ": " + keyCombination);
                    }
                    uniqueKeyCombinations.add(keyCombination);
                }

                excelData.add(rowData);
            }

            // If there are validation errors, throw exception with details including row and column information
            if (!validationErrors.isEmpty()) {
                StringBuilder errorMessage = new StringBuilder("Validation errors found:\n");
                for (ValidationError error : validationErrors) {
                    errorMessage.append(String.format("Row %d, Column %d (%s): %s (Value: '%s')\n", 
                        error.getRow(), error.getColumn(), error.getColumnName(), 
                        error.getErrorMessage(), error.getValue()));
                }
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, errorMessage.toString());
            }

        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Error reading Excel file: " + e.getMessage());
        }

        return excelData;
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        
        switch (cell.getCellType()) {
            case STRING:
                String stringValue = cell.getStringCellValue();
                return stringValue != null ? stringValue.trim() : "";
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                }
                // Handle numeric values carefully to avoid locale issues
                double numericValue = cell.getNumericCellValue();
                // If it's a whole number, return as integer string
                if (numericValue == Math.floor(numericValue)) {
                    return String.valueOf((long) numericValue);
                } else {
                    return String.valueOf(numericValue);
                }
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    // Try to evaluate the formula
                    switch (cell.getCachedFormulaResultType()) {
                        case STRING:
                            return cell.getStringCellValue().trim();
                        case NUMERIC:
                            double formulaNumeric = cell.getNumericCellValue();
                            if (formulaNumeric == Math.floor(formulaNumeric)) {
                                return String.valueOf((long) formulaNumeric);
                            } else {
                                return String.valueOf(formulaNumeric);
                            }
                        case BOOLEAN:
                            return String.valueOf(cell.getBooleanCellValue());
                        default:
                            return "";
                    }
                } catch (Exception e) {
                    // If formula evaluation fails, return the formula itself
                    return cell.getCellFormula();
                }
            case BLANK:
                return "";
            default:
                return "";
        }
    }

    private Object castValue(String value, String castTo) {
        if (castTo == null || value == null || value.trim().isEmpty()) {
            return value;
        }

        try {
            switch (castTo.toLowerCase()) {
                case "integer":
                case "int":
                    return Integer.parseInt(value.trim());
                case "long":
                    return Long.parseLong(value.trim());
                case "double":
                case "decimal":
                    return Double.parseDouble(value.trim());
                case "boolean":
                    return Boolean.parseBoolean(value.trim());
                default:
                    return value;
            }
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, 
                "Cannot cast value '" + value + "' to " + castTo);
        }
    }

    private List<Map<String, Object>> addAuditFields(List<Map<String, Object>> data, String createdBy) {
        LocalDateTime now = LocalDateTime.now();
        
        return data.stream().map(rowData -> {
            Map<String, Object> newRowData = new HashMap<>(rowData);
            newRowData.put("created_by", createdBy);
            newRowData.put("created_date", now);
            return newRowData;
        }).collect(Collectors.toList());
    }

    /**
     * Update table version management
     * 8. Update existing current version to set validity_end to current datetime
     * 9. Create new version with incremented version number
     */
    private String updateTableVersion(String tableName, String currentUser) {
        LocalDateTime now = LocalDateTime.now();
        
        int newVersionNumber = 1;
        int newSubVersionNumber = 0;
        // 8. Find and update current version (where validity_end is null)
        Optional<RuleAppTableVersionEntity> currentVersion = ruleAppTableVersionRepository.findFirstByTableIdAndValidityEndIsNullOrderByValidityStartDesc(tableName);
        if (currentVersion.isPresent()) {
            RuleAppTableVersionEntity version = currentVersion.get();
            version.setValidityEnd(now);
            version.setLastUpdatedBy(currentUser);
            version.setLastUpdatedDate(now);
            ruleAppTableVersionRepository.save(version);
            System.out.println("Updated existing version: " + version.getVersion() + "." + version.getSubVersion());
            
            
            if(currentVersion.get().getCreatedDate().toLocalDate().equals(LocalDate.now())) {
            	newSubVersionNumber = currentVersion.get().getSubVersion()+1;
            }else {
            	newVersionNumber = currentVersion.get().getVersion()+1;
            }
            RuleAppTableVersionEntity newVersion = new RuleAppTableVersionEntity();
            newVersion.setTableId(tableName);
            newVersion.setVersion(newVersionNumber);
            newVersion.setSubVersion(newSubVersionNumber);
            newVersion.setValidityStart(now);
            newVersion.setValidityEnd(null); // Current active version
            newVersion.setCreatedBy(currentUser);
            newVersion.setCreatedDate(now);
            
            ruleAppTableVersionRepository.save(newVersion);
            System.out.println("Created new version: " + newVersionNumber + "." + newSubVersionNumber);
        } else {
            System.out.println("No existing version found for table: " + tableName + ". Creating first version.");
            RuleAppTableVersionEntity newVersion = new RuleAppTableVersionEntity();
            newVersion.setTableId(tableName);
            newVersion.setVersion(newVersionNumber);
            newVersion.setSubVersion(newSubVersionNumber);
            newVersion.setValidityStart(now);
            newVersion.setValidityEnd(null); // Current active version
            newVersion.setCreatedBy(currentUser);
            newVersion.setCreatedDate(now);
            
            ruleAppTableVersionRepository.save(newVersion);
            System.out.println("Created new version: " + newVersionNumber + "." + newSubVersionNumber);
        } 
        
        return newVersionNumber+"."+newSubVersionNumber;
        
    }

    /**
     * Store historical data for audit purposes
     * 11. Package records in the form required for ruleapp_historical_data and persist
     */
    private void storeHistoricalData(String tableName, List<Map<String, Object>> data, String versionString, String currentUser) {
        LocalDateTime now = LocalDateTime.now();        
        
        try {
            
            // Convert the entire historical record to JSON string
            String jsonRecord = objectMapper.writeValueAsString(data);
            
            // Create and save the single historical data entity
            RuleappHistoricalDataEntity historicalData = new RuleappHistoricalDataEntity();
            historicalData.setTableId(tableName);
            historicalData.setVersion(versionString);
            historicalData.setRecord(jsonRecord);
            historicalData.setCreatedBy(currentUser);
            historicalData.setCreatedDate(now);
            
            ruleappHistoricalDataRepository.save(historicalData);
            
        System.out.println("Successfully stored historical data for version " + versionString);
        System.out.println("Total records in historical data: " + data.size());
        System.out.println("Historical data size: " + jsonRecord.length() + " characters");
        
    } catch (Exception e) {
        System.err.println("Error storing historical data: " + e.getMessage());
        e.printStackTrace();
        throw new RuntimeException("Failed to store historical data for table: " + tableName, e);
    }
}
} 