package com.example.demo.dto;

import lombok.Data;

@Data
public class ValidationError {
    private int row;
    private int column;
    private String columnName;
    private String value;
    private String errorMessage;
    
    public ValidationError(int row, int column, String columnName, String value, String errorMessage) {
        this.row = row;
        this.column = column;
        this.columnName = columnName;
        this.value = value;
        this.errorMessage = errorMessage;
    }
} 