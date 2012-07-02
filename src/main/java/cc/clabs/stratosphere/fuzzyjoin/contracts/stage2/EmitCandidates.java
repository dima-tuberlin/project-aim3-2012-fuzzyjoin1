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
package cc.clabs.stratosphere.fuzzyjoin.contracts.stage2;

import cc.clabs.stratosphere.fuzzyjoin.types.PactRecord;
import cc.clabs.stratosphere.fuzzyjoin.types.PactTokenlist;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Emits potential candidates based on the global token ordering.
 * Input Key is PactNull to ensure that we'll get the global token
 * ordering from the first stage into every user function. 
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class EmitCandidates extends MatchStub<PactNull, PactRecord, PactTokenlist, PactString, PactRecord> {
    
    private double THRESHOLD;
        
    @Override
    public void configure(Configuration parameters) {
        THRESHOLD = Double.parseDouble( parameters.getString( "THRESHOLD", "0") );
    }
    
    @Override
    public void match( PactNull key, PactRecord record, final PactTokenlist GLOBAL, Collector<PactString, PactRecord> collector ) {        
        // split value into possible prefix tokens
        String[] prefixes = canonicalize( record, GLOBAL );
        // calulate the number of needed prefix tokens
        int num_prefixes  = prefixes.length;
            num_prefixes -= (int) Math.ceil( THRESHOLD * (double) prefixes.length );
            num_prefixes += 1;
        // filter records that can never be matched …
        if ( prefixes.length < num_prefixes ) return;
        // finally emit all prefix tokens
        for (int i = 0; i < num_prefixes; i++)
            collector.collect( new PactString( prefixes[ i ] ), record);
    }
    
    private String[] canonicalize( PactRecord record, final PactTokenlist GLOBAL ) {
        // normalize the value
        String value = record.getValue().toLowerCase().replaceAll( "\\W", " " );
        String[] prefixes = value.split( " " );
        // remove duplicates
        ArrayList<String> tmplist = new ArrayList<String>() {};
        for ( String token : prefixes )
            if ( !tmplist.contains( token ) )
                tmplist.add( token );
        prefixes = tmplist.toArray( new String[0] );
        // order prefixes by global ordering
        Arrays.sort( prefixes, new Comparator<String>() {
            public int compare( String a, String b ) {
                int posA = GLOBAL.TOKENS.indexOf( a );
                int posB = GLOBAL.TOKENS.indexOf( b );
                if ( posA == -1 ) posA = Integer.MAX_VALUE;
                if ( posB == -1 ) posB = Integer.MAX_VALUE;
                return ( posA < posB ) ? -1 :
                       ( posA > posB ) ? 1 : 0;
            }
        });
        return prefixes;
    }

}