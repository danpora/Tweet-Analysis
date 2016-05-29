
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;


public class EC2Handler {
	private AmazonEC2 ec2;
	
	public EC2Handler(AWSCredentials credentials ) {
		ec2 = new AmazonEC2Client(credentials);    
		ec2.setEndpoint("ec2.us-east-1.amazonaws.com");
	}
	
    public void createInstance(int numOfInstToCreate) throws Exception {     
        try {
        	System.out.println("Launching required instance workers");
            // Basic 32-bit Amazon Linux AMI 1.0 (AMI Id: ami-08728661)
            RunInstancesRequest request = new RunInstancesRequest("ami-08111162", numOfInstToCreate, numOfInstToCreate);
            request.setInstanceType(InstanceType.T2Small.toString());
            request.withUserData(getUserDataScript());
            List<Instance> instances = ec2.runInstances(request).getReservation().getInstances();
            System.out.println("Launch instances: " + instances);
            
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
        }   
 
    }
    
    public int getRunningInstNum() {
    	int runningCount = 0;
    	DescribeInstancesRequest request = new DescribeInstancesRequest();
    	List<Reservation> result = ec2.describeInstances(request).getReservations();
    	for (Reservation reservation : result)
    		for (Instance instance : reservation.getInstances())
    			if (instance.getState().getCode() == 16)
    				runningCount ++;
    	return runningCount;
        }    
    
    public void terminateWorkerInstances() {
    	DescribeInstancesRequest request = new DescribeInstancesRequest();
    	List<Reservation> result = ec2.describeInstances(request).getReservations();
    	for (Reservation reservation : result){
    		ArrayList<String> instanceIDs = new ArrayList<String>();
    		for (Instance instance : reservation.getInstances()){
    			List<Tag> tags = instance.getTags();
    			if (tags.isEmpty())
    				instanceIDs.add(instance.getInstanceId());
					ec2.terminateInstances(new TerminateInstancesRequest(instanceIDs));
    		}
    	}	
    	System.out.println("####Terminating Workers####");
    }
    	  
static String getUserDataScript() throws IOException{
    	
    	
        BufferedReader br = new BufferedReader(new FileReader(new File("./credentials")));
        String line1 = br.readLine();
        String line2 = br.readLine();
        String line3 = br.readLine();
        br.close();

    	
    	
        ArrayList<String> lines = new ArrayList<>();
        lines.add("#! /bin/bash");

        lines.add("wget https://s3.amazonaws.com/deddydandsps162files/Worker.zip");
        lines.add("wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.3.0/stanford-corenlp-3.3.0-models.jar");
        lines.add("unzip Worker.zip");
        lines.add("sudo mv stanford-corenlp-3.3.0-models.jar Worker_lib");
        

        lines.add("echo " + line1 +  " > credentials");
        lines.add("echo " + line2 +  " >> credentials");
        lines.add("echo " + line3 +  " >> credentials");
        lines.add("java -jar Worker.jar");
        

        String str = new String (Base64.encodeBase64(join(lines, "\n").getBytes()));
        return str;
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