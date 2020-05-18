import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public Integer tag;                 // 0 for a graph vertex, 1 for a group number
    public Integer group;                // the group where this vertex belongs to
    public Integer VID;                  // the vertex ID
    public ArrayList<Integer> adjacent;     // the vertex neighbors
    
	public Vertex()
	{
		this.tag = 0;
		this.group = 0;
		this.VID = 0;
		this.adjacent = new ArrayList<Integer>();
	}
	
	public Vertex(Integer tag, Integer group, Integer VID, ArrayList<Integer> adjacent) 
	{
		this.tag = tag;
		this.group = group;
		this.VID = VID;
		this.adjacent = adjacent;
	}
	
	public Vertex(Integer tag, Integer group) 
	{
		this.tag = tag;
		this.group = group;
		this.VID = 0;
		this.adjacent = new ArrayList<Integer>();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException 
	{
		tag = in.readInt();
		group = in.readInt();
		VID = in.readInt();
		
		adjacent.clear();
		IntWritable size = new IntWritable();
		size.readFields(in);
		
		for(int i=0;i<size.get();i++) 
		{
			IntWritable adjacent_n = new IntWritable();
			adjacent_n.readFields(in);
			adjacent.add(adjacent_n.get());
		}
	}
	
	@Override
	public void write(DataOutput op) throws IOException 
	{
		op.writeInt(tag);
		op.writeInt(group);
		op.writeInt(VID);
	
		IntWritable size = new IntWritable(adjacent.size());
		size.write(op);
		
		for(Integer adjacent_n : adjacent) 
		{
			op.writeInt(adjacent_n);
		}
	}
	
	public Integer getTag()
	{
		return tag;
	}
	
	public Integer getGroup() 
	{
		return group;
	}
	
	public Integer getVID() 
	{
		return VID;
	}
	
	public ArrayList<Integer> getAdjacent() 
	{
		return adjacent;
	}
}

public class Graph 
{
    /* First Mapper */
	public static class MapVID extends Mapper<Object, Text, IntWritable, Vertex> 
	{
		@Override
		public void map(Object key, Text value, Context context)throws IOException, InterruptedException 
		{
			String readInput = value.toString();
			String[] tokenizeInput = readInput.split(",");
			
			Integer VID = new Integer (Integer.parseInt(tokenizeInput[0]));
			ArrayList<Integer> adjacent= new ArrayList<Integer>();
			for(int i=1;i<tokenizeInput.length;i++) 
			{
				adjacent.add(Integer.parseInt(tokenizeInput[i]));
			}
			
			Vertex v = new Vertex( 0, VID, VID, adjacent );
			context.write( new IntWritable(VID), v );
		}
	}
		
	/* Second Mapper and Reduce */
	public static class MapGroup extends Mapper<IntWritable, Vertex, IntWritable, Vertex> 
	{
		@Override
		public void map(IntWritable key, Vertex value, Context context) throws IOException, InterruptedException 
		{
			context.write(new IntWritable(value.getVID()), value);
		
			for(Integer adjacent : value.getAdjacent()) 
			{
				context.write(new IntWritable(adjacent),new Vertex(1,value.getGroup()));
			}
		}
	}
	
	public static class ReduceGroup extends Reducer<IntWritable, Vertex, IntWritable, Vertex> 
	{
		@Override
		public void reduce(IntWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException 
		{
			Integer setGroup = Integer.MAX_VALUE;
			ArrayList<Integer> adjacent = new ArrayList<Integer>();
		
			for(Vertex v : values) 
			{
				Integer tag = (Integer) v.getTag();
				if(tag.equals(0)) 
				{
					adjacent = new ArrayList<Integer>(v.getAdjacent());
				}
				setGroup = min(setGroup,v.getGroup());	
			}	
			Vertex setVertex = new Vertex(0, setGroup, key.get(), adjacent);
			context.write(new IntWritable(setGroup), setVertex);
		}
	
		public Integer min(Integer a,Integer b) 
		{
			if( a < b ) 
			{
				return a;
			} 
			else 
			{
				return b;
			}
		}
	}
	
	/* Third Mapper and Reducer  */
	public static class MapCount extends Mapper<IntWritable, Vertex, IntWritable, IntWritable> 
	{
		@Override
		public void map(IntWritable key, Vertex value, Context context) throws IOException, InterruptedException 
		{
			context.write(key, new IntWritable(1));
		}
	}
	
	public static class ReduceCount extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> 
	{
		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			Integer connectedComponents = 0;
		
			for (IntWritable value : values) 
			{
				connectedComponents += value.get();
			}
			context.write(key, new IntWritable(connectedComponents));
		}
	}

    public static void main ( String[] args ) throws Exception 
	{
		Job job = Job.getInstance();
		job.setJobName("Fisrt Mapper");
		job.setJarByClass(Graph.class);
		job.setMapperClass(MapVID.class);
		job.setNumReduceTasks(0);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Vertex.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Vertex.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/f0"));
		job.waitForCompletion(true);
		
		for(int i=0;i<5;i++) 
		{
			Job job1 = Job.getInstance();
			job1.setJobName("Second Mapper : Pass " + i);
			job1.setJarByClass(Graph.class);
			job1.setMapperClass(MapGroup.class);
			job1.setReducerClass(ReduceGroup.class);
			job1.setMapOutputKeyClass(IntWritable.class);
			job1.setMapOutputValueClass(Vertex.class);
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(Vertex.class);
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);
			FileInputFormat.setInputPaths(job1, new Path(args[1] + "/f" + i));
			FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/f" + (i+1)));
			job1.waitForCompletion(true);
		}
		
		Job job2 = Job.getInstance();
		job2.setJobName("Third Mapper");
		job2.setJarByClass(Graph.class);
		job2.setMapperClass(MapCount.class);
		job2.setReducerClass(ReduceCount.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f5"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));	
		job2.waitForCompletion(true);
		
    }
}
