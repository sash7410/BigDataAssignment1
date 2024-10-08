import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class IDAssigner {

    public static class FrequencyMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\t");
            if (tokens.length == 2) {
                word.set(tokens[0]);  
                count.set(Integer.parseInt(tokens[1]));  
                context.write(count, word);  
            }
        }
    }

    public static class IDReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private int idCounter = 1; 

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(new IntWritable(idCounter), val);  
                idCounter++;  
            }
        }
    }

    // Custom comparator for descending order
    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable k1 = (IntWritable) w1;
            IntWritable k2 = (IntWritable) w2;
            return -1 * k1.compareTo(k2);  // Multiply by -1 for descending order
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "ID Assigner");

        job.setJarByClass(IDAssigner.class);

        job.setMapperClass(FrequencyMapper.class);
        job.setReducerClass(IDReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //  custom comparator for descending order
        job.setSortComparatorClass(DescendingIntComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}