import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;

public class TaskReducer {
	
	private SQSFactory sqsFactory;
	private HashMap<String, Integer> filesMap;
	private SQSHandler sqsHandler;
	private TaskDATA taskData;
	
	public TaskReducer(SQSFactory sqsFactory, SQSHandler sqsTHandler, HashMap<String, Integer> filesMap, TaskDATA taskData ){
		// connecting to an SQS object
		this.sqsFactory = sqsFactory;
        this.filesMap = filesMap;
        this.sqsHandler = sqsTHandler;
        this.taskData = taskData;
	}
	
	public String run() {	
		while(true) {
			// Receive messages
			String sentiment = null;
			String entities = null;
			String tweet = null;
			String taskID = null;
	        System.out.println("Receiving results messages from MyResultQueue.\n");
	        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsFactory.getSqsUrl("Results"));      
	        List<Message> messages = sqsFactory.getSqsObject("Results").receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
	        for (Message message : messages) {
	        	tweet = message.getBody();
	        	sentiment = message.getMessageAttributes().get("sentiment").getStringValue();
                entities = message.getMessageAttributes().get("entities").getStringValue();
                taskID = message.getMessageAttributes().get("task").getStringValue();  

                msgToFile(tweet, entities, sentiment, taskID);  
               
                //delete result message
                String messageReceiptHandle = message.getReceiptHandle();
                
        		try {
					sqsFactory.getSqsObject("Results").deleteMessage(new DeleteMessageRequest(sqsFactory.getSqsUrl("Results"), messageReceiptHandle));
				} catch (Exception e) {
					System.out.println("##Error deleting job message from Results Queue###");
					e.printStackTrace();
				}
        		
                if (filesMap.get(taskData.getTaskID()) >= sqsHandler.getTaskSize()) {
                	return taskData.getTaskID();
                }
            }        	
		}    
	}
	
	public void msgToFile(String tweet, String entity, String sentiment, String taskID) {
		System.out.println("#####Writing result message to file..#####");
		try {
			filesMap.put(taskID, filesMap.get(taskID) + 1);
			try(FileWriter fw = new FileWriter(taskID + ".txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(tweet + " " + entity + " " + sentiment);    
				} catch (IOException e) {
				    System.out.println("Unable to steam to file");
				}
		} catch (Exception e1) {	
			e1.printStackTrace();
			System.out.println("The problematic taskID is : " + taskID);
		}	
	}
	
}
