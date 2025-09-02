package com.example.demo.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class BulkUploadFormColumn {
    @JsonProperty("excel_column_name")
    private String excelColumnName;
    
    @JsonProperty("column_name")
    private String columnName;
    
    @JsonProperty("regex_pattern")
    private String regexPattern;
    
    @JsonProperty("cast_to")
    private String castTo;
} 