package com.example.demo1;

import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class FileHelper {

    public static File MultipartFile2File(MultipartFile multipartFile) throws IOException {
        String fileName = multipartFile.getOriginalFilename();
        String prefix = fileName.substring(fileName.lastIndexOf("."));
        final File file = File.createTempFile(UUID.randomUUID().toString(), prefix);
        multipartFile.transferTo(file);
        return file;
    }
}