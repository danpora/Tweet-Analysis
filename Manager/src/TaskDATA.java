import com.amazonaws.services.sqs.model.Message;

public class TaskDATA {
	private String bucketName;
	private String fileKey;  
	private String workRatio; 
	private String taskID;
	private boolean isTerminate;
	private MyS3Object object;
	private int taskSize;
	
	public TaskDATA(String taskID, String bucketName, String fileKey, String workRatio, MyS3Object object, boolean isTerminate) {
		this.bucketName = bucketName;
		this.fileKey = fileKey;	
		this.workRatio = workRatio;
		this.isTerminate = isTerminate;
		this.taskID = taskID;
		this.object = object;
		this.taskSize = 0;
	}
	
	public TaskDATA() {
		this.bucketName = null;
		this.fileKey = null;
	}

	public TaskDATA(Message message) {
		bucketName = message.getMessageAttributes().get("bucketName").getStringValue();
		fileKey = message.getMessageAttributes().get("fileKey").getStringValue();
		workRatio = message.getMessageAttributes().get("workRatio").getStringValue();
		taskID = message.getMessageAttributes().get("taskID").getStringValue();
		object = new MyS3Object(bucketName, fileKey);
	}

	public String getBucketName() {
		return this.bucketName;
	}
	
	public String getKey() {
		return this.fileKey;
	}
	
	public String getWorkRatio() {
		return this.workRatio;
	}
	
	public boolean isValidTask() {
		return bucketName != null && fileKey != null;
	}
	
	public boolean isTerminate() {
		return this.isTerminate;
	}
	
	public String getTaskID() {
		return this.taskID;
	}
	
	public void setTaskSize(int taskSize) {
		this.taskSize = taskSize;
	}
	
	public int getTaskSize() {
		return taskSize; 
	}
	public MyS3Object getInputObject() {
		return object;
	}
}
