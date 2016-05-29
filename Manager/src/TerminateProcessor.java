import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import com.amazonaws.services.sqs.model.Message;

public class TerminateProcessor implements Runnable {
	
	private SQSHandler sqsHandler;
	private EC2Handler ec2handler;

	public TerminateProcessor(SQSHandler sqsHandler, EC2Handler ec2handler) {
		this.sqsHandler = sqsHandler;
		this.ec2handler = ec2handler;
	}

	@Override
	public void run() {
		for (int i = 0; i < ec2handler.getRunningInstNum(); i++)
	    	sqsHandler.sendSQSMessage("Jobs", "terminate");
		int incomeStats = 0;
		System.out.println("Running EC2 " + ec2handler.getRunningInstNum());
		while(incomeStats < ec2handler.getRunningInstNum() - 1) {
	    	List<Message> statMessageList = sqsHandler.getSQSMessage("Stats");
	    	if (!statMessageList.isEmpty()) {
	    		String statLine = statMessageList.get(0).getBody();
	    		if (statLine.equals("stat")){
	    			String stats = statMessageList.get(0).getMessageAttributes().get("result").getStringValue();
		    		msgToFile("Worker " + incomeStats + " " + stats);
		    		incomeStats ++;
		    		sqsHandler.deleteMessage(statMessageList.get(0), "Stats");
	    		}
	    	}
		} 
		System.out.println("###All statistics received###");
		MyS3Object mySummeryObject = new MyS3Object("deddy-dan-dsps162-statisticsbucket2", "WorkerStatistics", "WorkerStatistics.txt");
		mySummeryObject.upload();
		sqsHandler.sendFilePosition("resultStats", "deddy-dan-dsps162-statisticsbucket2", "WorkerStatistics", "Summary");
	}
	
	public void msgToFile(String statLine) {
		System.out.println("Writing worker statistic message to file..");
		try {
			try(FileWriter fw = new FileWriter("WorkerStatistics.txt", true);
				    BufferedWriter bw = new BufferedWriter(fw);
				    PrintWriter out = new PrintWriter(bw))
				{
				    out.println(statLine);    
				} catch (IOException e) {
				    System.out.println("Unable to steam to file");
				}
		} catch (Exception e1) {	
			e1.printStackTrace();
		}	
	}

}
