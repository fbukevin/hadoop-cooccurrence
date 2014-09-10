package cooc;

import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

public class myMapWritable extends MapWritable{
   @Override
   public String toString(){
      String s = new String("{ ");
      Set<Writable> keys = this.keySet();
      for(Writable key : keys){
         IntWritable count = (IntWritable) this.get(key);
         s =  s + key.toString() + "=" + count.toString() + ",";
      }

      s = s + " }";
      return s;
   }
}
