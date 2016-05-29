import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSHandler {
	private int msgCount = 0;
	private SQSFactory sqsFactory;

	public SQSHandler(SQSFactory sqsFactory){
		this.sqsFactory = sqsFactory;
	}
	
	public List<Message> getSQSMessage(String sqsName) {
		// Receive messages
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsFactory.getSqsUrl(sqsName));
        receiveMessageRequest.setMaxNumberOfMessages(1);
        List<Message> messages = sqsFactory.getSqsObject(sqsName).receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();
        return messages;  
	}
	
	public synchronized int serialObjectSend(String sqsName, TaskDATA taskDATA) throws IOException {
		
		System.out.println("Start loading job messages to jobs SQS..");
		
		while(true) {
			String line = taskDATA.getInputObject().read();
			if (line == null) break;
			msgCount ++;
		
			SendMessageRequest sendMessageRequest;
			
			try {
				sendMessageRequest = new SendMessageRequest(sqsFactory.getSqsUrl(sqsName), line); // send to jobs
				
				Map<String, MessageAttributeValue> sendMessageAttributes =
	                    new HashMap<String, MessageAttributeValue>();

	            sendMessageAttributes.put("task", new MessageAttributeValue()
	                    .withDataType("String")
	                    .withStringValue(taskDATA.getTaskID()));
	            
	            sendMessageRequest.setMessageAttributes(sendMessageAttributes);
				sqsFactory.getSqsObject(sqsName).sendMessage(sendMessageRequest);
				
			} catch (Exception e) {
				System.out.println("Error: your line is : `" + line + " and it made some troubles");
				e.printStackTrace();
			}			
		}
		
		System.out.println("Loading messages to jobs SQS complited");
		return msgCount;	
	}
	
	public synchronized void sendFilePosition(String messageBody, String bucketName, String fileName, String sqsName) {
		System.out.println("Sending file position to 'Summary' SQS..");

		SendMessageRequest sendMessageRequest = new SendMessageRequest(sqsFactory.getSqsUrl(sqsName), messageBody);

        Map<String, MessageAttributeValue> sendMessageAttributes =
                new HashMap<String, MessageAttributeValue>();

        sendMessageAttributes.put("fileKey", new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(fileName));
        
        sendMessageAttributes.put("bucketName", new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(bucketName));
        
        sendMessageRequest.setMessageAttributes(sendMessageAttributes);
        
        sqsFactory.getSqsObject(sqsName).sendMessage(sendMessageRequest);	
	}
	
	public synchronized void sendSQSMessage(String sqsName, String messageBody){
		SendMessageRequest sendMessageRequest = new SendMessageRequest(sqsFactory.getSqsUrl(sqsName), messageBody);
		sqsFactory.getSqsObject(sqsName).sendMessage(sendMessageRequest);			
	}
	
	public synchronized void deleteMessage(Message message, String sqsName) {
		// Delete a message
        String messageReceiptHandle = message.getReceiptHandle();
        sqsFactory.getSqsObject(sqsName).deleteMessage(new DeleteMessageRequest(sqsFactory.getSqsUrl(sqsName), messageReceiptHandle));
	}
	
	public synchronized int getTaskSize() {
		return msgCount;
	}

}
