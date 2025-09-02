package com.example.demo.dto;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;
import java.util.List;

@Data
public class ExcelUploadRequest {
    private List<String> groups;
    private MultipartFile file;
} 