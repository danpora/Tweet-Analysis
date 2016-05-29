import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.*;

/**
 * Created by deddy on 04/05/16.
 */
public class MyS3Object {
    private File object;
    private String fileName;
    BufferedReader reader;
    String bucketName;
    String fileKey;
    AmazonS3 s3;
    public String getBucketName() {
        return bucketName;
    }

    public String getFileKey() {
        return fileKey;
    }

    public MyS3Object(String bucketName, String fileKey ) {
        createS3();
        this.bucketName = bucketName;
        this.fileKey = fileKey;
        try {
            createFileFromInputStream(s3.getObject(new GetObjectRequest(bucketName, fileKey)).getObjectContent());
        } catch (IOException e) {
            e.printStackTrace();
        }
        object = new File("./tempcreateFileFromInputStream.txt");
        try {
            reader = new BufferedReader(new FileReader(object));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }


    public MyS3Object(String bucketName, String fileKey, String fileName) {
        createS3();
        this.bucketName = bucketName;
        this.fileKey = fileKey;
        this.fileName = fileName;
    }
    
    public synchronized void upload() {
    /*
     * Upload an object to bucket.
     */
    System.out.println("Uploading a new object to S3 from a file\n");
    File file = new File(this.fileName);
    s3.putObject(new PutObjectRequest(bucketName, fileKey, file));
    }
    
    public synchronized static void createFileFromInputStream(InputStream input) throws IOException
    {
        OutputStream fos = new FileOutputStream("./tempcreateFileFromInputStream.txt");
        BufferedOutputStream out = new BufferedOutputStream(fos);
        int count;
        byte[] buffer = new byte[32000]; // more if you like but no need for it to be the entire file size
        while ((count = input.read(buffer)) > 0)
        {
            out.write(buffer, 0, count);
        }

        out.close();
    }
    public synchronized String read() throws IOException {
        String line = reader.readLine();

        return line;
    }

    private void createS3(){

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

        Region usEast2 = Region.getRegion(Regions.US_EAST_1);
        s3 = new AmazonS3Client(credentials);
        s3.setRegion(usEast2);

    }
}
