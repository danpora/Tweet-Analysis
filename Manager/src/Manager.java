
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.sqs.model.Message;


public class Manager {
	
	public static void main(String[] args) throws Exception { 	
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
    
        SQSFactory sqsFactory = new SQSFactory(credentials);
        
        // create HashTable to hold summary files collection
        HashMap<String, Integer> filesMap = new HashMap<String, Integer>();
        
        EC2Handler ec2handler = new EC2Handler(credentials);
        
        // tasks thread pool
        ExecutorService executor = Executors.newFixedThreadPool(10);
        
        // SQS queue handler
        SQSHandler sqsHandler = new SQSHandler(sqsFactory);
        
        try {                    
            while(true) {
            	System.out.println("Searching for Task/Terminate messages..");
            	List<Message> messages = sqsHandler.getSQSMessage("Task");
            	if (!messages.isEmpty()) {
            		Message message = messages.get(0);
	            	if (message.getBody().equals("task")) {	
	            		System.out.println("***Got a TASK message***");
	            		sqsHandler.deleteMessage(message, "Task");
	            		// adding current task to task thread pool
	            		TaskDATA taskDATA = new TaskDATA(message);
	            		executor.execute(new TaskProcessor(taskDATA, sqsFactory, filesMap, ec2handler));
	            	} else if (message.getBody().equals("terminate")) {
	            		sqsHandler.deleteMessage(message, "Task");
	            		executor.shutdown();
	            		System.out.println("#Start Terminate process..#");
	        			System.out.println("#No more task will be accepted#");
	        		    System.out.println("#Waiting for previous tasks to complete..#");
	            		executor.awaitTermination(60, TimeUnit.SECONDS);  

	            		// termination process thread pool
	        	        ExecutorService terminationExecutor = Executors.newFixedThreadPool(10);
	        	        terminationExecutor.execute(new TerminateProcessor(sqsHandler, ec2handler));
	        	        terminationExecutor.shutdown();
	        	        terminationExecutor.awaitTermination(60, TimeUnit.SECONDS);
	        	        
	        	        // if all processes are done, shut all Workers and Manager down
	        	        while(true){
		        		    if (executor.isTerminated()) {
								ec2handler.terminateWorkerInstances();
								System.out.println("-- Manager is down --");
								return;
							}
	        	        }
	            	}    	 
            	}
            	Thread.sleep(2000);
         }
            	       
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it " +
                    "to Amazon SQS, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered " +
                    "a serious internal problem while trying to communicate with SQS, such as not " +
                    "being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
}
