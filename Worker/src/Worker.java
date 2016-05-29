import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.sqs.model.*;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;


import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by deddy on 04/05/16.
 */
public class Worker {


    public static void main(String[] args) {

        Worker worker = new Worker();

        System.out.println("===========================================");
        System.out.println("Worker is Up");
        System.out.println("===========================================\n");

        worker.run();

        System.out.println("===========================================");
        System.out.println("Worker is Down");
        System.out.println("===========================================\n");

    }

    AmazonSQS sqs;
    String outStatsQueueUrl, outJobsQueueUrl, inJobsQueueUrl;


    private int totalNumberOfTweets = 0,faultyTweets = 0,okTweet = 0;

    StanfordCoreNLP  sentimentPipeline;
    StanfordCoreNLP NERPipeline;
    public Worker() {
        createQ();

        Properties props = new Properties();
        props.put("annotators", "tokenize, ssplit, parse, sentiment");
        sentimentPipeline =  new StanfordCoreNLP(props);


        props = new Properties();
        props.put("annotators", "tokenize , ssplit, pos, lemma, ner");
        NERPipeline =  new StanfordCoreNLP(props);

   }

    public void run(){

        Boolean finished = false;
        int sentiment;
        String entities,task;
        String tweet;
        String body;
        Message message;
        List<Message> messages;
        ReceiveMessageRequest receiveMessageRequest;

        System.out.println("===========================================");
        System.out.println("And running");
        System.out.println("===========================================\n");
        long startTime, workTime = 0;
        while(!finished) {

            System.out.println("===========================================");
            System.out.println("Receiving messages from MyQueue.");
            System.out.println("===========================================\n");
            startTime = System.currentTimeMillis();
            receiveMessageRequest = new ReceiveMessageRequest(inJobsQueueUrl);
            
            receiveMessageRequest.setMaxNumberOfMessages(1);
            
            messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("All")).getMessages();

            if (!messages.isEmpty()) {
                message = messages.get(0);

                System.out.println("  Message");
                System.out.println("    MessageId:     " + message.getMessageId());
                System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());


                body = message.getBody();
                if (body.equals("terminate")) {
                	System.out.println("Worker got TERMINATE MESSAGE*******************");
                    // Send a message




                    SendMessageRequest sendMessageRequest = new SendMessageRequest(outStatsQueueUrl, "stat");

                    Map<String, MessageAttributeValue> sendMessageAttributes =
                            new HashMap<String, MessageAttributeValue>();

                    sendMessageAttributes.put("result", new MessageAttributeValue()
                            .withDataType("String")
                            .withStringValue("Total Number Of Tweets: " + totalNumberOfTweets + " Tweets finished successfully: " + okTweet + " Tweets failed: " + faultyTweets + " Total Working Time: " +workTime + "ms."));

                    sendMessageRequest.setMessageAttributes(sendMessageAttributes);

                    sqs.sendMessage(sendMessageRequest);

                    finished = true;
                }
                else{
                        // Work on tweet
                        System.out.println("===========================================");
                        System.out.println("Working on tweet.");
                        System.out.println("===========================================\n");
                        totalNumberOfTweets++;
                        task = message.getMessageAttributes().get("task").getStringValue();

                    try {
                        tweet = getTweet(message.getBody());

                        System.out.println("tweet: " + tweet);
                        sentiment = findSentiment(tweet);
                        System.out.println("tweet ranking: " + sentiment);
                        entities = printEntities(tweet);
                        System.out.println("Entities: " + entities);
                        System.out.println("Task: " + task);

                        // Send result
                        System.out.println("===========================================");
                        System.out.println("Send result.");
                        System.out.println("===========================================\n");
                        sendResult(tweet,sentiment,entities,task);

                    } catch (IOException e) {
                        sendResult("Error 404",2," ",task);
                        faultyTweets++;
                        e.printStackTrace();
                    }
                    
                    
                        // Delete a message
                        System.out.println("===========================================");
                        System.out.println("Deleting a message.");
                        System.out.println("===========================================\n");
                        String messageReceiptHandle = message.getReceiptHandle();
                        sqs.deleteMessage(new DeleteMessageRequest(inJobsQueueUrl, messageReceiptHandle));
                        okTweet++;
                        workTime += System.currentTimeMillis() - startTime;
                }
            }

            else {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }

        }
    }

    private void sendResult(String tweet, int sentiment, String entities, String task) {
        System.out.println("Tweet: " + tweet + ". Sentiment: " + sentiment + " entities: " + entities);


        SendMessageRequest sendMessageRequest = new SendMessageRequest(outJobsQueueUrl, tweet);


        Map<String, MessageAttributeValue> sendMessageAttributes =
                new HashMap<String, MessageAttributeValue>();

        sendMessageAttributes.put("entities", new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(entities));
        sendMessageAttributes.put("sentiment", new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(String.valueOf(sentiment)));
        sendMessageAttributes.put("task", new MessageAttributeValue()
                .withDataType("String")
                .withStringValue(task));
        sendMessageRequest.setMessageAttributes(sendMessageAttributes);

        sqs.sendMessage(sendMessageRequest);


    }

    private void createQ(){
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
        Region usEast1 = Region.getRegion(Regions.US_EAST_1);

        sqs = new AmazonSQSClient(credentials);
        sqs.setRegion(usEast1);


        System.out.println("Creating a new SQS queue called MyJobsQueue.\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("MyJobsQueue");
        inJobsQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        System.out.println("Created SQS queue called MyJobsQueue.\nURL: " + inJobsQueueUrl + "\n");


        System.out.println("Creating a new SQS queue called MyResultsQueue.\n");
        createQueueRequest = new CreateQueueRequest("MyResultsQueue");
        outJobsQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        System.out.println("Created SQS queue called MyResultsQueue.\nURL: " + inJobsQueueUrl + "\n");



        System.out.println("Creating a new SQS queue called MyStatsQueue.\n");
        createQueueRequest = new CreateQueueRequest("MyStatsQueue");
        outStatsQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl();

        System.out.println("Created SQS queue called MyStatsQueue.\nURL: " + outStatsQueueUrl + "\n");

    }

    public static String getTweet(String url) throws IOException {
        Document doc;
        doc = Jsoup.connect(url).get();
        return doc.title();
    }

    public String  printEntities(String tweet){
        StringBuilder result = new StringBuilder("[");

        System.out.println("===========================================");
        System.out.println("Getting Entities from tweet");
        System.out.println("===========================================\n");


        // create an empty Annotation just with the given text
        Annotation document = new Annotation(tweet);

        // run all Annotators on this text
        NERPipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        for(CoreMap sentence: sentences) {
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                String word = token.get(TextAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                System.out.println("\t-" + word + ":" + ne);
                if (!ne.equals("O")){

                    if (result.length() != 1) result.append(",");
                    result.append(word);
                    result.append(":");
                    result.append(ne);
                }
            }
        }
        result.append("]");
        return result.toString();

    }

    private int findSentiment(String tweet) {

        System.out.println("===========================================");
        System.out.println("Find Sentiment from tweet");
        System.out.println("===========================================\n");

        int mainSentiment = 0;
        if (tweet != null && tweet.length() > 0) {
            int longest = 0;
            Annotation annotation = sentimentPipeline.process(tweet);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.AnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }

            }
        }
        return mainSentiment;
    }
}
