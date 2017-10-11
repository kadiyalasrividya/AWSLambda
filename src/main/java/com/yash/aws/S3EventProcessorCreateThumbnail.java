package com.yash.aws;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClient;
import com.amazonaws.services.sns.model.CreateTopicRequest;
import com.amazonaws.services.sns.model.CreateTopicResult;
import com.amazonaws.services.sns.model.PublishRequest;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3EventProcessorCreateThumbnail implements RequestHandler<S3Event, String> {
    public String handleRequest(S3Event s3event, Context context) {
        try {
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            // Object key may have spaces or unicode non-ASCII characters.
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            // Download the image from S3 into a stream
            AmazonS3 s3Client = new AmazonS3Client();
            S3Object s3Object = s3Client.getObject(new GetObjectRequest(
                    srcBucket, srcKey));
            BufferedReader br = null;
            int sum = 0;
            try {
                br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));
                String sCurrentLine;
                //Sum numbers from file
                while (((sCurrentLine = br.readLine()) != null) && (!sCurrentLine.equalsIgnoreCase("END"))) {
                    sum += Integer.parseInt(sCurrentLine);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (br != null)
                        br.close();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            //Get topic
            AmazonSNS client = new AmazonSNSClient();
            CreateTopicRequest createTopicRequest = new CreateTopicRequest("SrividyaTopic");
            CreateTopicResult arn = client.createTopic(createTopicRequest);
            //Publish sum to subscribers
            PublishRequest publishRequest = new PublishRequest(arn.getTopicArn(), "The sum of all numbers was " + sum);
            client.publish(publishRequest);
            return "Ok";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}







