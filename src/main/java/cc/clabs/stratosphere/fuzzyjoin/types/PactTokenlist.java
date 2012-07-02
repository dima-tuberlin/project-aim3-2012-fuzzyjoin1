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

import eu.stratosphere.pact.common.type.Value;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PactTokenlist implements Value {

    public ArrayList<String> TOKENS = new ArrayList<String>();
    
    public PactTokenlist() { super(); }
    
    public PactTokenlist( ArrayList<String> tokens ) {
        TOKENS.addAll( tokens );
    }
    
    public PactTokenlist( String[] tokens ) {
        TOKENS = ( ArrayList<String> ) Arrays.asList( tokens );
    }
    
    public void write( DataOutput out ) throws IOException {
        out.writeInt( TOKENS.size() );
        for ( String token : TOKENS)
            out.writeUTF( token );
    }

    public void read( DataInput in ) throws IOException {
        int num = in.readInt();
        for ( int i = 0; i < num; i++ )
            TOKENS.add( in.readUTF() );
        
    }
    
    @Override
    public boolean equals( Object that ) {
        return ( that instanceof PactTokenlist ) ?
               TOKENS.equals( ((PactTokenlist) that).TOKENS ) :
               false;
    }

    @Override
    public int hashCode() {
        return TOKENS.hashCode();
    }

}