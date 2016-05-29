import java.io.IOException;

public class TaskExecutor {
	private SQSHandler sqsHandler;
	private EC2Handler ec2Handler;
	public int jobs_per_worker = 100;
	private TaskDATA taskDATA;
	
	public TaskExecutor(SQSHandler sqsHandler, EC2Handler ec2Handler, TaskDATA taskDATA) {
		this.ec2Handler = ec2Handler;
		this.sqsHandler = sqsHandler;
		this.jobs_per_worker = Integer.parseInt(taskDATA.getWorkRatio());
		this.taskDATA = taskDATA;
	}

	public void run() throws IOException {
		taskDATA.setTaskSize(sqsHandler.serialObjectSend("Jobs", taskDATA));
		System.out.println("Adding requierd EC2 instances..");
		addEC2Instances();	
	}
	
	public void addEC2Instances() {
		int currentInstRunning = ec2Handler.getRunningInstNum();
		int currentTaskWorkers = (sqsHandler.getTaskSize() / jobs_per_worker) + 2;
		int instToCreate = 0;
		if (currentTaskWorkers - currentInstRunning >= 0)
				instToCreate = currentTaskWorkers - currentInstRunning;
		else
			instToCreate = 0;
		System.out.println("Task workers needed : " + currentTaskWorkers);
		System.out.println("Cuurent running : " + currentInstRunning);
		System.out.println("Instances to create : " + instToCreate);
		if (instToCreate > 0) {
			try {
				ec2Handler.createInstance(instToCreate);
			} catch (Exception e) {
				System.out.println("Couldn't create EC2 instances");
				e.printStackTrace();
			}
		}
	}

}
