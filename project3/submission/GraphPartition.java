import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


/*exd7040*/
class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */
    public Vertex()
    {
    	adjacent = new Vector<>();
    	this.id=0;
    	this.adjacent=null;
    	this.centroid=-1;
    	this.depth=0;
    }
    public Vertex(long id, Vector<Long> adj, long centroid, short depth)
    {
    	adjacent = new Vector<>();
    	this.id = id;
    	this.adjacent = adj;
    	this.centroid = centroid;
    	this.depth = depth;    	
    }
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		adjacent = new Vector<>();
		this.id = in.readLong();
		this.centroid = in.readLong();		
		this.depth = in.readShort();
		long nav = in.readLong();
		for(int i=0;i<nav;i++)
			this.adjacent.addElement(in.readLong());
				
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeLong(adjacent.size());
		for(int i=0;i<adjacent.size();i++)
		{
			out.writeLong(adjacent.get(i));			
		}				
	}
	public String toString()
	{
		if(adjacent==null)
			return Long.toString(id)+","+ null+","+Long.toString(centroid)+","+Short.toString(depth);
		return Long.toString(id)+","+ adjacent.toString()+","+Long.toString(centroid)+","+Short.toString(depth);
	}
       
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int nav1=0;
    /* ... */
    public static class GraphMapper1 extends Mapper<Object,Text,LongWritable,Vertex>
    {
    	private LongWritable id = new LongWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    	{
    		Scanner my_scan = new Scanner(value.toString()).useDelimiter(",");    		
    		long centroid = -1;    		
    		long nav7 = my_scan.nextLong(); 
    		id.set(nav7);    			
    		Vector<Long> adj = new Vector<Long>();
    		while(my_scan.hasNext())
    			adj.add(my_scan.nextLong());
    		if(nav1<10)
    		{    			
    			centroid = nav7;
    			centroids.add(nav7);
    			nav1++;    			
    			context.write(id, new Vertex(nav7,adj,centroid,BFS_depth));
    		}  
    		else
    			context.write(id, new Vertex(nav7,adj,centroid,BFS_depth));
    	}
    }    
    
    public static class GraphMapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>
    {    	
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException
    	{    						
			Vector<Long> nav4 = new Vector<Long>();						
    		context.write(new LongWritable(value.id), value);    		
    		if(value.centroid>0)    		
    			for(long l:value.adjacent)    			    		
    				context.write(new LongWritable(l), new Vertex(l,nav4,value.centroid,BFS_depth));    		
    	}
    }
    
    public static class GraphReducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>
    {
    	public void reduce(LongWritable key, Iterable<Vertex> values,Context context) throws IOException, InterruptedException
    	{
    		short nav6=1000;    		    		
    		Vertex nav5 = new Vertex(key.get(),new Vector<>(),-1,(short)0);
    		for(Vertex v:values)
    		{    			
    			if(!v.adjacent.isEmpty())
    				nav5.adjacent=v.adjacent;    			
    			if(v.centroid>0 && v.depth<nav6)
    			{
    				nav6 = v.depth;
    				nav5.centroid = v.centroid;
    			}
    		}
    		nav5.depth = nav6;
    		context.write(key,nav5);
    	}
    }
    
    public static class GraphMapper3 extends Mapper<LongWritable, Vertex, LongWritable, IntWritable>
    {
    	public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException
    	{    		    		    		   		    		 		    		    		
    		if(value.centroid>=0)
    			context.write(new LongWritable(value.centroid), new IntWritable(1));
    	}
    }
    public static class GraphReducer3 extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable>
    {
    	public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    	{
    		int nav5=0;
    		for(IntWritable v:values)
    			nav5=nav5+v.get();
    		context.write(key, new IntWritable(nav5));
    	}
    }
    
    public static void main ( String[] args ) throws Exception {
        Job job = Job.getInstance();
        job.setJobName("Eshan Graph Partition");
        job.setJarByClass(GraphPartition.class);
        /* ... First Map-Reduce job to read the graph */
        job.setMapperClass(GraphMapper1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class); 
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);              
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        job.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
	        job.setJarByClass(GraphPartition.class);
            job.setReducerClass(GraphReducer1.class);
            job.setOutputValueClass(Vertex.class); 
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);           
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapperClass(GraphMapper2.class);
            FileInputFormat.setInputPaths(job, new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */      
job.setJarByClass(GraphPartition.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setReducerClass(GraphReducer3.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(GraphMapper3.class);
        FileInputFormat.setInputPaths(job, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}
