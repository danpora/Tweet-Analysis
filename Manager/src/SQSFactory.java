import java.util.HashMap;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class SQSFactory {
	
	private AWSCredentials credentials;
	private HashMap<String, AmazonSQS> sqsMap = new HashMap<String, AmazonSQS>();
	
	public SQSFactory(AWSCredentials credentials) {
		this.credentials = credentials;
		createBasicSQSs();
	}
	
	public void createBasicSQSs() {
		AmazonSQS tasksSQS = new AmazonSQSClient(credentials);
        AmazonSQS jobsSQS = new AmazonSQSClient(credentials);
        AmazonSQS resultSQS = new AmazonSQSClient(credentials);
        AmazonSQS summarySQS = new AmazonSQSClient(credentials);
        AmazonSQS statsSQS = new AmazonSQSClient(credentials);
        Region usEast2 = Region.getRegion(Regions.US_EAST_1);
        tasksSQS.setRegion(usEast2);
        jobsSQS.setRegion(usEast2);
        resultSQS.setRegion(usEast2);
        summarySQS.setRegion(usEast2);
        statsSQS.setRegion(usEast2);
        sqsMap.put("Task", tasksSQS);
        sqsMap.put("Jobs", jobsSQS);
        sqsMap.put("Results", resultSQS);
        sqsMap.put("Summary", summarySQS);
        sqsMap.put("Stats", summarySQS);
        System.out.println("*** All SQS's are defined and set ***");
	}
	
	public AmazonSQS getSqsObject(String sqsName) {
		return sqsMap.get(sqsName);
	}
	
	public String getSqsUrl(String sqsName) {
		CreateQueueRequest createTaskQueueRequest = new CreateQueueRequest("My" + sqsName + "Queue");
        return sqsMap.get(sqsName).createQueue(createTaskQueueRequest).getQueueUrl();
	}
}
