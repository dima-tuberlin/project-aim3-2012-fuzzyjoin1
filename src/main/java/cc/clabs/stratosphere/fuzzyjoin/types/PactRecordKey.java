/**
 *  ______ __  __  ______  ______  __  __     __  ______  __  __   __
 * /\  ___/\ \/\ \/\___  \/\___  \/\ \_\ \   /\ \/\  __ \/\ \/\ "-.\ \
 * \ \  __\ \ \_\ \/_/  /_\/_/  /_\ \____ \ _\_\ \ \ \/\ \ \ \ \ \-.  \
 *  \ \_\  \ \_____\/\_____\/\_____\/\_____/\_____\ \_____\ \_\ \_\\"\_\
 *   \/_/   \/_____/\/_____/\/_____/\/_____\/_____/\/_____/\/_/\/_/ \/_/
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob@CLABS.CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package cc.clabs.stratosphere.fuzzyjoin.types;

import eu.stratosphere.pact.common.type.Key;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PactRecordKey implements Key {

    public String DATASET_NAME;
    public int RECORD_ID;
    
    public PactRecordKey() { super(); }
    
    public PactRecordKey( String name, Integer id ) {
        DATASET_NAME = name;
        RECORD_ID = id;
    }
    
    public void write( DataOutput out ) throws IOException {
        out.writeUTF( DATASET_NAME );
        out.writeInt( RECORD_ID );
    }

    public void read( DataInput input ) throws IOException {
        DATASET_NAME = input.readUTF();
        RECORD_ID = input.readInt();
    }
    
    @Override
    public boolean equals( Object other ) {
        return ( other instanceof PactRecordKey ) ?
               this.hashCode() == other.hashCode() :
               false;
    }

    @Override
    public int hashCode() {
        return ( DATASET_NAME + RECORD_ID ).hashCode();
    }

    public int compareTo( Key other ) {
        PactRecordKey that = ( PactRecordKey ) other;
        int name = DATASET_NAME.compareTo( that.DATASET_NAME );
        return ( name != 0 ) ? name :
               ( RECORD_ID == that.RECORD_ID ) ? 0 :
               ( RECORD_ID > that.RECORD_ID ) ? 1 : -1;
    }
    
    @Override
    public String toString() {
        return DATASET_NAME + RECORD_ID;
    }

}