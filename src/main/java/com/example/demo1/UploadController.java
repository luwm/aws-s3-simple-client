package com.example.demo1;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletResponse;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

@RestController
@Slf4j
public class UploadController {
    @Value("${my.aws.accessKey}")
    private String accessKey;

    @Value("${my.aws.secretKey}")
    private String secretKey;

    @Value("${my.aws.hostname}")
    private String hostname;

    @GetMapping("/list")
    public List<Bucket> listBuckets() {
        return createConnect().listBuckets();
    }

    @PostMapping("/create")
    public Bucket createBucket(String bucketName) {
        return createConnect().createBucket(bucketName);
    }

    @GetMapping("/{buckName}/list")
    public ObjectListing listObjects(@PathVariable(name = "buckName") String buckName) {
        return createConnect().listObjects(buckName);
    }

    @PostMapping("/{buckName}/putobject")
    public PutObjectResult putObject(@RequestParam("file") MultipartFile file, @PathVariable(name = "buckName") String buckName) throws FileNotFoundException {
        File tempFile = null;
        try {
            tempFile = FileHelper.MultipartFile2File(file);
            log.info("filename: {}", tempFile.getName());
            return createConnect().putObject(
                    buckName, tempFile.getName(), tempFile);
        }catch (Exception e) {
            log.error("statck trace: {}", e.fillInStackTrace());
            log.error("message: {}", e.getMessage());
        }finally {
            if (null!= tempFile){
                tempFile.delete();
            }
        }
        return null;
    }

    @GetMapping(value = "/{bucketName}/object/{objectName}")
    public void getObject(HttpServletResponse response, @PathVariable(name = "bucketName") String bucketName, @PathVariable(name = "objectName") String objectName) throws Exception {
        OutputStream outputStream = null;
        log.info("bucketName:{}, objectName:{}", bucketName, objectName);
        S3Object o = createConnect().getObject(bucketName, objectName);
        log.info("object:{}", o.toString());
        S3ObjectInputStream s3is = o.getObjectContent();
        BufferedImage image =  ImageIO.read(s3is);
        response.setContentType("image/png");
        outputStream = response.getOutputStream();
        if(image!=null) {
            ImageIO.write(image, "png", outputStream);
        }
    }

    @PostMapping("/{buckName}/multipartUpload")
    public CompleteMultipartUploadResult multipartUpload(@RequestParam("file") MultipartFile file, @PathVariable(name = "buckName") String buckName) throws Exception{
        File tempFile = FileHelper.MultipartFile2File(file);
        long contentLength = tempFile.length();
        // Set part size to 5 MB.
        long partSize = 5 * 1024 * 1024;
        AmazonS3 s3Client = createConnect();
        List<PartETag> partETags = new ArrayList<PartETag>();

        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(buckName, tempFile.getName());
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        log.info("start upload getUploadId: {}", initResponse.getUploadId());
        // Upload the file parts.
        long filePosition = 0;
        for (int i = 1; filePosition < contentLength; i++) {
            // Because the last part could be less than 5 MB, adjust the part size as needed.
            partSize = Math.min(partSize, (contentLength - filePosition));
            log.info("starting upload seq: {}", i);
            // Create the request to upload a part.
            UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(buckName)
                    .withKey(tempFile.getName())
                    .withUploadId(initResponse.getUploadId())
                    .withPartNumber(i)
                    .withFileOffset(filePosition)
                    .withFile(tempFile)
                    .withPartSize(partSize);

            // Upload the part and add the response's ETag to our list.
            UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
            partETags.add(uploadResult.getPartETag());

            filePosition += partSize;
        }

        // Complete the multipart upload.
        CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(buckName, tempFile.getName(),
                initResponse.getUploadId(), partETags);
        CompleteMultipartUploadResult result = s3Client.completeMultipartUpload(compRequest);
        log.info("end upload result: {}", result.getLocation());
        return result;
    }

    private AmazonS3 createConnect() {
        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds))
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(hostname,"")
                ).build();
        return s3Client;
    }
}
