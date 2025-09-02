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
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
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

    private static final Logger log = LoggerFactory.getLogger(ExcelUploadService.class);

    private final LookUpTableRepository lookUpTableRepository;
    private final RuleAppTableVersionRepository ruleAppTableVersionRepository;
    private final RuleappHistoricalDataRepository ruleappHistoricalDataRepository;
    private final DynamicTableService dynamicTableService;
    private final ObjectMapper objectMapper;

    public ExcelUploadService(LookUpTableRepository lookUpTableRepository,
                              RuleAppTableVersionRepository ruleAppTableVersionRepository,
                              RuleappHistoricalDataRepository ruleappHistoricalDataRepository,
                              DynamicTableService dynamicTableService) {
        this.lookUpTableRepository = lookUpTableRepository;
        this.ruleAppTableVersionRepository = ruleAppTableVersionRepository;
        this.ruleappHistoricalDataRepository = ruleappHistoricalDataRepository;
        this.dynamicTableService = dynamicTableService;

        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Transactional
    public Map<String, Object> uploadExcelFile(String tableName, List<String> groups, MultipartFile file) {
        LookUpTableEntity lookUpTable = lookUpTableRepository.findByTableName(tableName)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Table not found: " + tableName));

        if (!hasUploadPermission(lookUpTable.getUploadableByGroups(), groups)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "User not authorized to upload to this table");
        }

        List<BulkUploadFormColumn> bulkUploadForm = parseBulkUploadForm(lookUpTable.getBulkUploadForm());
        List<String> keyColumns = parseKeyColumns(lookUpTable.getKeyColumns());

        List<Map<String, Object>> excelData = validateAndExtractExcelData(file, bulkUploadForm, keyColumns);

        String currentUser = "currentUser"; // TODO: Replace with SecurityContext
        List<Map<String, Object>> finalData = addAuditFields(excelData, currentUser);

        log.info("Preparing to insert {} rows into {}", finalData.size(), tableName);

        String versionString = updateTableVersion(tableName, currentUser);

        dynamicTableService.truncateAndReloadTable(tableName, finalData, bulkUploadForm, currentUser);

        storeHistoricalData(tableName, finalData, versionString, currentUser);

        return Map.of(
                "message", "Excel file processed successfully",
                "totalRows", finalData.size(),
                "version", versionString
        );
    }

    private boolean hasUploadPermission(String uploadableByGroups, List<String> userGroups) {
        if (uploadableByGroups == null || userGroups == null) return false;
        try {
            List<String> allowedGroups = objectMapper.readValue(uploadableByGroups, new TypeReference<>() {});
            return userGroups.stream().anyMatch(allowedGroups::contains);
        } catch (Exception e) {
            log.error("Error parsing uploadableByGroups: {}", e.getMessage());
            return false;
        }
    }

    private List<BulkUploadFormColumn> parseBulkUploadForm(String json) {
        if (json == null || json.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Bulk upload form is null or empty");
        }
        try {
            List<BulkUploadFormColumn> result = objectMapper.readValue(json, new TypeReference<>() {});
            if (result.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Bulk upload form is empty after parsing");
            }
            return result;
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid bulk upload form format", e);
        }
    }

    private List<String> parseKeyColumns(String json) {
        if (json == null || json.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Key columns is null or empty");
        }
        try {
            List<String> result = objectMapper.readValue(json, new TypeReference<>() {});
            if (result.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Key columns are empty after parsing");
            }
            return result;
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid key columns format", e);
        }
    }

    private List<Map<String, Object>> validateAndExtractExcelData(
            MultipartFile file, List<BulkUploadFormColumn> bulkUploadForm, List<String> keyColumns) {

        List<Map<String, Object>> excelData = new ArrayList<>();
        List<ValidationError> validationErrors = new ArrayList<>();
        Map<String, Integer> keyOccurrences = new HashMap<>();

        try (Workbook workbook = new XSSFWorkbook(file.getInputStream())) {
            Sheet sheet = workbook.getSheetAt(0);
            if (sheet.getPhysicalNumberOfRows() < 2) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Excel must contain headers and at least one row");
            }

            Map<String, Integer> columnIndexMap = extractHeaderMapping(sheet.getRow(0), bulkUploadForm);

            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (row == null) continue;

                Map<String, Object> rowData = processRow(row, bulkUploadForm, columnIndexMap, validationErrors, rowIndex);
                String key = buildKey(rowData, keyColumns);

                if (key != null && keyOccurrences.containsKey(key)) {
                    throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                            String.format("Duplicate key '%s' at rows %d and %d", key, keyOccurrences.get(key), rowIndex + 1));
                }
                keyOccurrences.put(key, rowIndex + 1);
                excelData.add(rowData);
            }

            if (!validationErrors.isEmpty()) {
                throw new ResponseStatusException(HttpStatus.BAD_REQUEST, formatValidationErrors(validationErrors));
            }
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Error reading Excel file", e);
        }
        return excelData;
    }

    private Map<String, Integer> extractHeaderMapping(Row headerRow, List<BulkUploadFormColumn> form) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null) columnIndexMap.put(cell.getStringCellValue().trim(), i);
        }
        Set<String> expectedColumns = form.stream().map(BulkUploadFormColumn::getExcelColumnName).collect(Collectors.toSet());
        if (!columnIndexMap.keySet().containsAll(expectedColumns)) {
            expectedColumns.removeAll(columnIndexMap.keySet());
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Missing required columns: " + expectedColumns);
        }
        return columnIndexMap;
    }

    private Map<String, Object> processRow(Row row, List<BulkUploadFormColumn> form,
                                           Map<String, Integer> columnIndexMap,
                                           List<ValidationError> errors, int rowIndex) {
        Map<String, Object> rowData = new HashMap<>();
        for (BulkUploadFormColumn col : form) {
            Integer columnIndex = columnIndexMap.get(col.getExcelColumnName());
            if (columnIndex == null) continue;

            String cellValue = getCellValueAsString(row.getCell(columnIndex));
            if (col.getRegexPattern() != null && !cellValue.isEmpty() && !Pattern.matches(col.getRegexPattern(), cellValue)) {
                errors.add(new ValidationError(rowIndex + 1, columnIndex + 1, col.getExcelColumnName(),
                        cellValue, "Value does not match regex pattern"));
            }
            rowData.put(col.getColumnName(), castValue(cellValue, col.getCastTo()));
        }
        return rowData;
    }

    private String buildKey(Map<String, Object> row, List<String> keyColumns) {
        if (keyColumns == null || keyColumns.isEmpty()) return null;
        return keyColumns.stream().map(k -> Objects.toString(row.get(k), "")).collect(Collectors.joining("|"));
    }

    private String formatValidationErrors(List<ValidationError> errors) {
        return errors.stream()
                .map(e -> String.format("Row %d, Col %d (%s): %s [Value='%s']",
                        e.getRow(), e.getColumn(), e.getColumnName(), e.getErrorMessage(), e.getValue()))
                .collect(Collectors.joining("; "));
    }

    private String getCellValueAsString(Cell cell) {
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().trim();
            case NUMERIC -> DateUtil.isCellDateFormatted(cell) ? cell.getDateCellValue().toString()
                    : (cell.getNumericCellValue() == Math.floor(cell.getNumericCellValue())
                    ? String.valueOf((long) cell.getNumericCellValue())
                    : String.valueOf(cell.getNumericCellValue()));
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            case FORMULA -> cell.getCellFormula();
            default -> "";
        };
    }

    private Object castValue(String value, String castTo) {
        if (value == null || value.isBlank() || castTo == null) return value;
        try {
            return switch (castTo.toLowerCase()) {
                case "int", "integer" -> Integer.parseInt(value.trim());
                case "long" -> Long.parseLong(value.trim());
                case "double", "decimal" -> Double.parseDouble(value.trim());
                case "boolean" -> Boolean.parseBoolean(value.trim());
                default -> value;
            };
        } catch (NumberFormatException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Cannot cast value '" + value + "' to " + castTo);
        }
    }

    private List<Map<String, Object>> addAuditFields(List<Map<String, Object>> data, String createdBy) {
        LocalDateTime now = LocalDateTime.now();
        return data.stream().map(row -> {
            Map<String, Object> newRow = new HashMap<>(row);
            newRow.put("created_by", createdBy);
            newRow.put("created_date", now);
            return newRow;
        }).toList();
    }

    private String updateTableVersion(String tableName, String currentUser) {
        LocalDateTime now = LocalDateTime.now();
        int version = 1, subVersion = 0;

        Optional<RuleAppTableVersionEntity> current = ruleAppTableVersionRepository
                .findFirstByTableIdAndValidityEndIsNullOrderByValidityStartDesc(tableName);

        if (current.isPresent()) {
            RuleAppTableVersionEntity existing = current.get();
            existing.setValidityEnd(now);
            existing.setLastUpdatedBy(currentUser);
            existing.setLastUpdatedDate(now);
            ruleAppTableVersionRepository.save(existing);

            if (existing.getCreatedDate().toLocalDate().equals(LocalDate.now())) {
                version = existing.getVersion();
                subVersion = existing.getSubVersion() + 1;
            } else {
                version = existing.getVersion() + 1;
            }
        }

        RuleAppTableVersionEntity newVersion = new RuleAppTableVersionEntity();
        newVersion.setTableId(tableName);
        newVersion.setVersion(version);
        newVersion.setSubVersion(subVersion);
        newVersion.setValidityStart(now);
        newVersion.setCreatedBy(currentUser);
        newVersion.setCreatedDate(now);
        ruleAppTableVersionRepository.save(newVersion);

        return version + "." + subVersion;
    }

    private void storeHistoricalData(String tableName, List<Map<String, Object>> data, String version, String currentUser) {
        try {
            String jsonRecord = objectMapper.writeValueAsString(data);
            RuleappHistoricalDataEntity historical = new RuleappHistoricalDataEntity();
            historical.setTableId(tableName);
            historical.setVersion(version);
            historical.setRecord(jsonRecord);
            historical.setCreatedBy(currentUser);
            historical.setCreatedDate(LocalDateTime.now());
            ruleappHistoricalDataRepository.save(historical);
        } catch (Exception e) {
            throw new RuntimeException("Failed to store historical data for " + tableName, e);
        }
    }
}
