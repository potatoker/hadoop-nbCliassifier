package ray;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair>{
	private Text first;
	private Text second;
	
	public TextPair(){
		set(new Text(), new Text());
	}
	
	public TextPair(TextPair pair2){
		first = new Text(pair2.first.toString());
		second = new Text(pair2.second.toString());
	}
	
	public TextPair(String first, String second){
		set(new Text(first), new Text(second));
	}
	
	public TextPair(Text first, Text second){
		set(first, second);
	}
	
	public Text getFirst(){
		return first;
	}
	
	public Text getSecond(){
		return second;
	}
	
	private void set(Text first, Text second) {
		// TODO Auto-generated method stub
		this.first = first;
		this.second = second;
	}
	
	public String toString(){
		return first + "&" + second;
	}
	
	public Text formatPair(){
		return new Text(first + "&" + second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(TextPair tp) {
		// TODO Auto-generated method stub
		int cmp = first.compareTo(tp.first);
		if(cmp != 0){
			return cmp;
		}
		return second.compareTo(tp.second);
	}
	
	public static class Comparator extends WritableComparator{
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		
		public Comparator(){
			super(TextPair.class);
		}
		
		public int compare(byte[] b1, int s1, int i1,  byte[] b2, int s2, int i2){
			try{
				int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				int cmp = TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				
				if(cmp !=0) {
					return cmp;
				}
			    return TEXT_COMPARATOR.compare(b1, s1+firstL1, i1-firstL1, b2, s2+firstL2, i2-firstL2);
				
			}catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}		
	}
	
	static { WritableComparator.define(TextPair.class, new Comparator());}

//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = 1;
//		result = prime * result + ((first == null) ? 0 : first.hashCode());
//		result = prime * result + ((second == null) ? 0 : second.hashCode());
//		return result;
//	}
	
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
            // if deriving: appendSuper(super.hashCode()).
            append(first).
            append(second).
            toHashCode();
    }

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TextPair other = (TextPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	
	

}
