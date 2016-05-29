/*
 * Copyright 2010-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;


import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.*;

import org.apache.commons.codec.binary.Base64;

import java.awt.Desktop;


public class Local {
	
	static long lStartTime;

    public static void main(String[] args) throws IOException, URISyntaxException {
    	
    	lStartTime = System.currentTimeMillis();
    	
        String taskID = UUID.randomUUID().toString();
        MyS3Object Stats = null;
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("./credentials", "default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (/users/studs/bsc/2015/davidzag/.aws/credentials), and is in valid format.",
                    e);
        }
        
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);

        /*
         * S3:
         */
        AmazonS3 s3 = new AmazonS3Client(credentials);
        s3.setRegion(usEast1);


        /*
         * SQS:
         */
        AmazonSQS sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(usEast1);


        String bucketName = "my-first-s3-bucket-" + UUID.randomUUID();

        String fileKey = "MyObjectKey";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon");
        System.out.println("===========================================\n");

        createMangerNode();

        try {
            /*
             * Create a new S3 bucket.
             */
            System.out.println("Creating bucket " + bucketName + "\n");
            s3.createBucket(bucketName);


            // Create Task queue
            System.out.println("Creating a new SQS queue called MyTaskQueue.\n");
            CreateQueueRequest createTaskQueueRequest = new CreateQueueRequest("MyTaskQueue");
            String myTaskQueueUrl = sqs.createQueue(createTaskQueueRequest).getQueueUrl();

            System.out.println("Created SQS queue called MyTaskQueue.\nURL: " + myTaskQueueUrl + "\n");

            // Create Summary queue
            System.out.println("Creating a new SQS queue called MySummaryQueue.\n");
            CreateQueueRequest createSummaryQueueRequest = new CreateQueueRequest("MySummaryQueue");
            String mySummaryQueueUrl = sqs.createQueue(createSummaryQueueRequest).getQueueUrl();

            System.out.println("Created SQS queue called MySummaryQueue.\nURL: " + mySummaryQueueUrl + "\n");


            /*
             * Upload an object to bucket.
             */
            MyS3Object uploadFile = new MyS3Object(bucketName,fileKey,args[0]);


            // Send a message
            System.out.println("Sending a message to MyQueue. taskID: " + taskID + " \n");
            SendMessageRequest sendMessageRequest = new SendMessageRequest(myTaskQueueUrl, "task");

            Map<String, MessageAttributeValue> sendMessageAttributes =
                    new HashMap<String, MessageAttributeValue>();

            sendMessageAttributes.put("bucketName", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(bucketName));
            sendMessageAttributes.put("fileKey", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(fileKey));
            sendMessageAttributes.put("workRatio", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(args[2]));
            sendMessageAttributes.put("taskID", new MessageAttributeValue()
                    .withDataType("String")
                    .withStringValue(taskID));
            sendMessageRequest.setMessageAttributes(sendMessageAttributes);

            sqs.sendMessage(sendMessageRequest);


            /*
            *
            * WAIT FOR END
            *
            */

            // Receive messages
            Boolean fineshed = false;
            String resultBucketName, resultFileKey;
            while(!fineshed) {

                try {
                    TimeUnit.SECONDS.sleep(15);                } catch(InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }

                System.out.println("Receiving messages from MyQueue.\n");
                ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(mySummaryQueueUrl);

                List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
                for (Message message : messages) {
                    System.out.println("  Message");
                    System.out.println("    MessageId:     " + message.getMessageId());
                    System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                    System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                    System.out.println("    Body:          " + message.getBody());
                    for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                        System.out.println("  Attribute");
                        System.out.println("    Name:  " + entry.getKey());
                        System.out.println("    Value: " + entry.getValue());
                    }
                    if (message.getBody().equals("resultMSG")) {


                        System.out.println("===========================================");
                        System.out.println("Got result MSG!");
                        System.out.println("===========================================\n");


                        Map<String, MessageAttributeValue> recievedMessageAttributes = message.getMessageAttributes();
                        resultFileKey = recievedMessageAttributes.get("fileKey").getStringValue();

                        if (resultFileKey.equals(taskID)) {
                            System.out.println("===========================================");
                            System.out.println("Got result for my task");
                            System.out.println("===========================================\n");


                            System.out.println("result: " + message.getBody());


                            resultBucketName = recievedMessageAttributes.get("bucketName").getStringValue();
                            System.out.println("resultBucketName: " + resultBucketName);

                            System.out.println("resultFileKey: " + resultFileKey);

                            System.out.println("===========================================");
                            System.out.println("Download summary file");
                            System.out.println("===========================================\n");

                            MyS3Object object = new MyS3Object(resultBucketName, resultFileKey);

                            // Delete a message
                            System.out.println("Deleting a message.\n");
                            String messageReceiptHandle = message.getReceiptHandle();
                            sqs.deleteMessage(new DeleteMessageRequest(mySummaryQueueUrl, messageReceiptHandle));



                            if (args.length == 4 && args[3].equals("terminate")) {
                                System.out.println("===========================================");
                                System.out.println("Start terminate");
                                System.out.println("===========================================\n");

                                Stats = terminationProcess(sqs);
                            }


                            System.out.println("===========================================");
                            System.out.println("Create Html File");
                            System.out.println("===========================================\n");

                            createHtmlFile(object, args[1], Stats);


                            System.out.println("===========================================");
                            System.out.println("Open summary file");
                            System.out.println("===========================================\n");
                            
                            fineshed = true;

                        }
                    }

                }
                System.out.println();
            }





            System.out.println("- end message.\n");



            deleteBucket(s3,bucketName);



        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }

    private static MyS3Object terminationProcess(AmazonSQS sqs) {
        boolean isTerminated = false;
        MyS3Object object = null;
        String totalWorkTime = "0", totalTweets = "0", totalGoodTweets = "0", totalFailedTweets = "0";
        String statsFileKey, statsBucketName;

        // Create Stats queue
        System.out.println("Creating a new SQS queue called MyStatsQueue.\n");
        CreateQueueRequest createSummaryQueueRequest = new CreateQueueRequest("MySummaryQueue");
        String mySummaryQueueUrl = sqs.createQueue(createSummaryQueueRequest).getQueueUrl();

        System.out.println("Created SQS queue called MySummaryQueue.\nURL: " + mySummaryQueueUrl + "\n");

        System.out.println("Sending terminate message to MyQueue.\n");
        sqs.sendMessage( new SendMessageRequest(sqs.createQueue(new CreateQueueRequest("MyTaskQueue")).getQueueUrl(), "terminate"));


        while(!isTerminated) {

            try {
                TimeUnit.SECONDS.sleep(15);                } catch(InterruptedException ex) {
                Thread.currentThread().interrupt();
            }

            System.out.println("Receiving messages from MyQueue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(mySummaryQueueUrl);

            List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
            for (Message message : messages) {
                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                for (Entry<String, MessageAttributeValue> entry : message.getMessageAttributes().entrySet()) {
                    System.out.println("  Attribute");
                    System.out.println("    Name:  " + entry.getKey());
                    System.out.println("    Value: " + entry.getValue());
                }
                if (message.getBody().equals("resultStats")) {


                    System.out.println("===========================================");
                    System.out.println("Got Stats MSG!");
                    System.out.println("===========================================\n");

                    Map<String, MessageAttributeValue> recievedMessageAttributes = message.getMessageAttributes();
                    statsFileKey = recievedMessageAttributes.get("fileKey").getStringValue();
                    statsBucketName = recievedMessageAttributes.get("bucketName").getStringValue();

                    object = new MyS3Object(statsBucketName, statsFileKey);



                    // Delete a message
                    System.out.println("Deleting a message.\n");
                    String messageReceiptHandle = message.getReceiptHandle();
                    sqs.deleteMessage(new DeleteMessageRequest(mySummaryQueueUrl, messageReceiptHandle));

                    return object;
                }

            }
            System.out.println();
        }

        return object;
    }

    private static void createHtmlFile(MyS3Object objectContent, String outputFile, MyS3Object stats) {

        System.out.println("===========================================");
        System.out.println("Create html file");
        System.out.println("===========================================\n");
        int numberOfTweets = 0;
        double avg = 0.0;
        File f = new File(outputFile);
        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new FileWriter(f));
            bw.write("<html>");
            bw.write("<body>");
            bw.write("<h1>Dan & Deddy's tweeter analysis:</h1>");
            bw.write("<br>");
            bw.newLine();
            if (stats != null){
                bw.write("<h2>Statistics:</h2> <br><br>");

                while(true) {
                    String line = stats.read();
                    if (line == null) break;

                    bw.write(line + "<br>");
                    bw.newLine();
                }
                bw.write("<br><br>");
            }
            bw.write("<h2>Tweets:</h2>");
            while (true) {
                String line = objectContent.read();//= reader.readLine();
                if (line == null) break;
                numberOfTweets++;
                switch (line.charAt(line.length()-1)) {
                    case '0':
                        bw.write("<font color=\"#af0000\">");
                        System.out.println("<font color=\"#af0000\">");
                        break;
                    case '1':
                        bw.write("<font color=\"#ff0000\">");
                        System.out.println("<font color=\"#ff0000\">");
                        avg +=1;
                        break;
                    case '2':
                        bw.write("<font color=\"#000000\">");
                        System.out.println("<font color=\"#000000\">");
                        avg +=2;
                        break;
                    case '3':
                        bw.write("<font color=\"#50d050\">");
                        System.out.println("<font color=\"#50d050\">");
                        avg +=3;
                        break;
                    case '4':
                        bw.write("<font color=\"#008015\">");
                        System.out.println("<font color=\"#008015\">");
                        avg +=4;
                        break;
                }

                bw.write(line.substring(0,line.length()-2)  + "</font><br><br>");
                bw.newLine();
                System.out.println(line + System.getProperty("line.separatpr") + "</font>");
            }
            System.out.println();
            avg = avg/numberOfTweets;
            bw.write("Total number of Tweets fot this task was: " + numberOfTweets + "The avarage sentiment is: " + avg);
            bw.newLine();
            bw.write("</body>");
            bw.write("</html>");

            bw.close();
            
            long lEndTime = System.currentTimeMillis();
            
            long diff = (lEndTime - lStartTime) / 1000;
            
            System.out.println("Total time to execute program : " + diff + " milliseconds");
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    static void deleteBucket(AmazonS3 s3, String bucketName){
        ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                .withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            s3.deleteObject(bucketName, objectSummary.getKey());

        }

        System.out.println();


        s3.deleteBucket(bucketName);

        System.out.println("===========================================");
        System.out.println("Deleted bucket " + bucketName);
        System.out.println("===========================================\n");

    }
    static String getUserDataScript () throws IOException {

/*
        BufferedReader br = new BufferedReader(new FileReader(new File("./credentials")));
        String line1 = br.readLine();
        String line2 = br.readLine();
        String line3 = br.readLine();
        br.close();
*/

        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");
        lines.add("wget  https://s3-us-west-2.amazonaws.com/deddy-dan-dsps162-sourcebucket/Manager.zip");
        lines.add("unzip -P deddydan8887 Manager.zip");
        lines.add("java -jar Manager.jar");
        String str = new String (Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
    }
    
    static void createMangerNode() throws IOException {


        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("./credentials", "default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                            "Please make sure that your credentials file is at the correct " +
                            "location (~/.aws/credentials), and is in valid format.",
                    e);
        }

        // Create the AmazonEC2Client object so we can call various APIs.
        AmazonEC2 ec2 = new AmazonEC2Client(credentials);
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);

        ec2.setRegion(usEast1);

        ec2.setEndpoint("ec2.us-east-1.amazonaws.com");

        if (!isManagerRunning(ec2)) {

            System.out.println("===========================================");
            System.out.println(" Starting Manager");
            System.out.println("===========================================\n");

            RunInstancesRequest request = new RunInstancesRequest("ami-08111162", 1, 1);

            request.setInstanceType(InstanceType.T2Small.toString());

            request.setUserData(getUserDataScript());

            RunInstancesResult ans = ec2.runInstances(request);
            String instanceid = ans.getReservation().getInstances().get(0).getInstanceId();
            ArrayList<String> instanceIdList = new ArrayList<>();
            ArrayList<Tag> tagList = new ArrayList<>();
            Tag tag = new Tag(instanceid, "manager");
            tagList.add(tag);
            instanceIdList.add(instanceid);
            ec2.createTags(new CreateTagsRequest(instanceIdList, tagList));
        }

        System.out.println("===========================================");
        System.out.println(" Manager is running");
        System.out.println("===========================================\n");


    }

    private static boolean isManagerRunning(AmazonEC2 ec2) {
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        List<Reservation> result = ec2.describeInstances(request).getReservations();
        for (Reservation reservation : result) {
            for (Instance instance : reservation.getInstances()) {
                List<Tag> tags = instance.getTags();
                for (Tag tag: tags) {
                    if (tag.getValue().equals("manager")){
                        if (instance.getState().getCode() == 16) {
                            return true;
                        }
                    }
                }

            }
        }
        return false;
    }

    static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext()) {
                break;
            }
            builder.append(delimiter);
        }
        return builder.toString();
    }

}
