import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;

public class StreamingPipeline {

    private static TupleTag<BqSchema> ValidMessage= new TupleTag<BqSchema>() {};
    private static TupleTag<String> InValidMessage=new TupleTag<String>(){};
    private static final Logger LOG = LoggerFactory.getLogger(StreamingPipeline.class);


    public interface PipelineOptions extends DataflowPipelineOptions {

        @Description("BigQuery table")
        String getOutputTableName();
        void setOutputTableName(String OutputTableName);

        @Description("input Subscription to read message")
        String getsubscription();
        void setsubscription(String subscription);

        @Description("DLQ topic")
        String getdlqTopic();
        void setdlqTopic(String dlqTopic);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(PipelineOptions.class);
        //with validation will check for each option.
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(PipelineOptions.class);
        run(options);
    }
    static class jsonValidator extends DoFn<String,BqSchema> {
        @ProcessElement
        public void processElement(@Element String jsonmessage,ProcessContext processContext)throws Exception{
            try {
                Gson gson = new Gson();
                BqSchema bqSchema = gson.fromJson(jsonmessage, BqSchema.class);
                processContext.output(ValidMessage,bqSchema);
            }catch(Exception e){
                e.printStackTrace();
                processContext.output(InValidMessage,jsonmessage);
            }
            }

    }

    // Schema for JSON to row conversion with three different fields and data types .

    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    /**
     * This method will create pipeline
     * check for the valid or invalid message
     * write accordingly to bigQuery Table or DLQ topic.
     */
    public static PipelineResult run(PipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        //Read data from pubsub subscription.
        PCollectionTuple pubsubMessage = pipeline
                .apply("Read Message From PubSub Subscription", PubsubIO.readStrings()
                        .fromSubscription(options.getsubscription()/*"projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-18"*/))

                //Filter data into two cateogory (VALID and INVALID).
                .apply("Check for Validity ", ParDo.of(new jsonValidator()).withOutputTags(ValidMessage, TupleTagList.of(InValidMessage)));

        //get PCollection<String> for both VALID and INVALID Data.
        PCollection<BqSchema> validData=pubsubMessage.get(ValidMessage);

        PCollection<String> invalidData=pubsubMessage.get(InValidMessage);

        validData.apply("Convert GSON to JSON",ParDo.of(new DoFn<BqSchema, String>() {
                    @ProcessElement
            public void convert(ProcessContext context){
                        Gson g = new Gson();
                        String gsonString = g.toJson(context.element());
                        context.output(gsonString);
                    }
        })).apply("JsonToRow",JsonToRow.withSchema(rawSchema)).
                apply("Write Message to table",BigQueryIO.<Row>write().to(options.getOutputTableName()/*"nttdata-c4e-bde:uc1_18.account"*/)
                        .useBeamSchema()
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        //write Invalid data(malformed data) to Big query.
        invalidData.apply("SendInValidMessageToDLQ",PubsubIO.writeStrings().to(options.getdlqTopic()/*"projects/nttdata-c4e-bde/topics/uc1-dlq-topic-18"*/));

        return pipeline.run();

    }
}