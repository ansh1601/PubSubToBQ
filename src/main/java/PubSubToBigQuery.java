import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.Row;

public class PubSubToBigQuery {


    //    Setting the pipelineOptions
    public interface MyOptions extends DataflowPipelineOptions {



        @Description("BigQuery table name")
        String getOutputTableName();
        void setOutputTableName(String outputTableName);

        @Description("PubSub Subscription")
        String getSubscription();
        void setSubscription(String subscription);
    }



    public static final Schema account=Schema.
            builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    public static void main(String[] args) {
        System.out.println("Hello");


        //Getting Options mentioned in command
        MyOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(MyOptions.class);
        Pipeline p=Pipeline.create(options);
        p.apply("GetDataFromPUBSub", PubsubIO.readStrings().fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-18"))
                .apply("ConvertToRow", JsonToRow.withSchema(account))
                .apply("WriteToBigqury", BigQueryIO.<Row>write().to("nttdata-c4e-bde:uc1_18.account").useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        p.run().waitUntilFinish();
    }
}