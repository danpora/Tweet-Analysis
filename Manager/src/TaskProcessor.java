import java.io.IOException;
import java.util.HashMap;

public class TaskProcessor implements Runnable {
	
	private TaskDATA taskDATA;
	private SQSFactory sqsFactory;
	private HashMap<String, Integer> filesMap;
	private EC2Handler ec2handler;
	
	public TaskProcessor(TaskDATA taskDATA, SQSFactory sqsFactory, HashMap<String, Integer> filesMap, EC2Handler ec2handler){
		this.taskDATA = taskDATA;
		this.sqsFactory = sqsFactory;
		this.filesMap = filesMap;
		this.ec2handler = ec2handler;
	}

	@Override
	public void run() {
		filesMap.put(taskDATA.getTaskID(), 0);
		SQSHandler sqsHandler = new SQSHandler(sqsFactory);    
		TaskExecutor taskExe = new TaskExecutor(sqsHandler, ec2handler, taskDATA);            		
		try {
			taskExe.run();
		} catch (IOException e) {
			System.out.println("Error while executing task");
			e.printStackTrace();
		}
		TaskReducer taskReducer = new TaskReducer(sqsFactory, sqsHandler, filesMap, taskDATA);
		String summaryFile = taskReducer.run();
		System.out.println("SUMMARY FILE TO UPLOAD: " + summaryFile);
		MyS3Object mySummeryObject = new MyS3Object(taskDATA.getBucketName(), summaryFile, summaryFile + ".txt");
		mySummeryObject.upload();
		sqsHandler.sendFilePosition("resultMSG", taskDATA.getBucketName(), taskDATA.getTaskID(), "Summary");
		
	}
}
