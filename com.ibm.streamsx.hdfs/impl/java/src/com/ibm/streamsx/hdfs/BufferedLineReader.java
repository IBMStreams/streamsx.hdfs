package com.ibm.streamsx.hdfs;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.hadoop.fs.FSDataInputStream;

public class BufferedLineReader implements Closeable{
	 static final int DEFAULT_BUFFER_SIZE = 64*1024;
		final static byte NEWLINE=0xA;
		final static byte CARRIAGE_RETURN=13;
	 int pos = 0;
	 int limit = 0;
	 byte buffer[] = new byte[DEFAULT_BUFFER_SIZE];
	 boolean eatFirstNewline = false;
	 final long startPos;
	 final long endPos;
	 boolean inputDone = false;
	 final FSDataInputStream in;
	 long bytesConsumed;
	 boolean aligned;
	 final Charset charSet;
	 
	 public BufferedLineReader(InputStream dataStream, long myStart, long myEnd,
			String fEncoding) throws IOException {
			in = (FSDataInputStream)dataStream;
			this.startPos = myStart;
			this.endPos = myEnd;
			bytesConsumed = 0;
			aligned = false;
			charSet  = Charset.forName(fEncoding);
			if (myStart > 0) {
				// we need to advance to the first newline.  If the newline is on the boundary, 
				// then we need to know that, so we need to read one before our official start. 
				in.seek(myStart-1);
				
				bytesConsumed = -1;
				// ready will be true when we found the first newline.
				boolean ready = false;
				while (!ready) {
				    // fill the buffer
					fillBuffer(false);
					// read the buffer, looking for a newline or carriage return.  This is safe for UTF-8.
				for (int i = pos; i < limit; i++) {
					if (buffer[i] == NEWLINE || buffer[i] == CARRIAGE_RETURN) {
						// Found a line end!  Set the position and the bytesConsumed.
						pos = i+1;
						ready = true;
						break;
					}
				}
				if (!ready) {
					pos = limit;	
				}
				bytesConsumed += pos;
				// if we didn't set ready yet, then we need to fill the buffer again and try again.
				}
				
				// we found a newline or a carriage return.
				// now we have to check whether the new character is a newline.
				if (pos == limit) {
					eatFirstNewline = true;
				}
				else {
					if (buffer[pos] == NEWLINE) {
						pos++;
						bytesConsumed++;
					}
				}
			}
			System.out.println("Start position was "+startPos+" consumed "+bytesConsumed+" to get to position "+pos);
	}

	 public long getPos() {
		 return bytesConsumed + startPos;
	 }

	 
	 private void fillBuffer(boolean grow ) throws IOException {
		 System.out.println("Need to fill buffer pos "+pos+" limit "+limit);
		 if (grow) {
		 		// In this case, we tried to get a line and couldn't get it, so our lines are super 
		 		// long.
		 	    byte[] oldbuffer = buffer;
		 	    buffer = new byte[2*oldbuffer.length];
		 	    for (int i = 0; i < limit-pos; i++) {
		 	    	buffer[i] = oldbuffer[i+pos];
		 	    }
		 	}
		 	else {
		 		System.out.println("Copying "+(limit-pos)+" bytes to front of buffer");	
		 		// just move stuff from teh end of hte buffer to the beginning
		 		for (int i = 0; i < limit - pos; i++) {
		 			buffer[i] = buffer[pos+i];
		 		}
		 	}
		 	int firstFree = limit - pos;
			int numRead =in.read(buffer,firstFree,buffer.length-firstFree);
			pos = 0;
			limit = firstFree + numRead;
			System.out.println("Pos is now "+pos+" limit is now "+limit);
			if (numRead< 0) {
				inputDone = true;
				return;
			}
			if (firstFree == 0 && eatFirstNewline && buffer[0] == NEWLINE) {
				bytesConsumed++;
				pos++;
			}
		}
 
	  public String readLine() throws IOException {
  
		  if (bytesConsumed >= (endPos - startPos) || (inputDone && pos==limit)) {
			  System.out.println("Have consumed "+bytesConsumed+" end pos was "+(endPos-startPos));
			  return null;
		  }
		  
		int stringLen = -1;
		for (int i = pos; i < limit; i++) {
			if (buffer[i] == NEWLINE || buffer[i] == CARRIAGE_RETURN) {
				stringLen= i-pos;
				break;
			}
		}
		if (stringLen < 0 && inputDone) {
			bytesConsumed += (limit - pos);
			if (limit - pos <= 0) {
				return null;
			}
			else {
				String toReturn = new String(buffer,pos,limit-pos);
				pos = limit;
				return toReturn;
			}
		}
		
		if (stringLen >= 0) {
			bytesConsumed += stringLen +1;
			String toReturn = new String(buffer,pos,stringLen);
			// now adjust pos.
			
			// CASE 1: last character read was a newline.
			if (buffer[stringLen+pos] == NEWLINE) {
				// this is fine whether we hit the end of the buffer or not.
				pos = pos + stringLen +1;
			}
			else if (buffer[stringLen+pos] == CARRIAGE_RETURN) {
				// Lines can end with CR, or CR/NEWLINE. 
				// If there is a newline, we should eat it.
				// first, if we're far from buffer end...
				if (stringLen+pos +1 < limit) {
					if( buffer[stringLen+pos+1] ==NEWLINE) {
						bytesConsumed++;;
						pos = stringLen+pos+2;
					}
				   else 
	
					   pos = stringLen+pos+1;
				} 
				else {
					pos = stringLen +1;
					// we'll need to eat the first character if it's a newline.
					eatFirstNewline = true;
				}
			}
			else {
				System.out.println("pos "+pos+" limit "+limit+" stringLen "+stringLen+" last char "+buffer[stringLen+pos]);
				throw new RuntimeException(" You have a bug.");
			}
			return toReturn;
		}
		else {
			if (pos == 0) {
				System.out.println("No line found in buffer");
				// Can't make a line, need to fill up buffer.
				fillBuffer(true);
			}
			else {
				fillBuffer(false);
			}
			return readLine();
		}
	 }
	  
	 public void close() {
		  try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
	 
}

