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
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordAndRecord;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordKey;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordJoinKeysAndSimilarity;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.OutputContract.*;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactNull;
import java.util.*;

/**
 * Compares a candidate pair. At first candidates
 * pairs must pass the filters. After that, the
 * similarity of the remaining candidate pair will
 * be measured. All pairs with a similarity greater
 * equal THRESHOLD will be emitted to the next stage.
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
@UniqueKey
public class FilterAndValidateCandidates extends ReduceStub<PactRecordAndRecord, PactNull, PactRecordKey, PactRecordJoinKeysAndSimilarity> {

    private double THRESHOLD;

    @Override
    public void configure( Configuration parameters ) {
        THRESHOLD = Double.parseDouble( parameters.getString( "THRESHOLD", "0") );
    }
    
    @Override
    public void reduce( PactRecordAndRecord records, Iterator<PactNull> wayne, Collector<PactRecordKey, PactRecordJoinKeysAndSimilarity> collector ) {
        PactRecord a = records.getFirst();
        PactRecord b = records.getSecond();
        // generate tokenlists
        String[] x = a.getValue().split( " " );
        String[] y = b.getValue().split( " " );
        // check for filters
        if ( !pass_filters( x, y ) ) return;
        // calculate similarity
        double sim = similarity( x, y );
        if ( sim < THRESHOLD ) return;
        // a composite of the RecordKey tuple and the similarity value
        PactRecordJoinKeysAndSimilarity value = new PactRecordJoinKeysAndSimilarity
        ( a.getRecordKey(), b.getRecordKey(), new PactDouble( sim ) );
        // emit for both sides …
        collector.collect( a.getRecordKey() , value );
        collector.collect( b.getRecordKey() , value );
    }
    
    private boolean pass_filters( String[] x, String[] y ) {
        // filter by length
        if ( THRESHOLD * x.length > y.length ) return false;
        if ( THRESHOLD * y.length > x.length ) return false;
        return true;
    }
    
    /**
     * Calculates the similarity based on the
     * Jaccard index.
     * 
     * @param x First Tokenlist.
     * @param y Second Tokenlist.
     * @return A similarity value between 0 and 1.
     */
    private double similarity( String[] x, String[] y) {
        List<String> a = Arrays.asList( x );
        List<String> b = Arrays.asList( y );
        // calculate the intersection of x and y
        int intersection = 0;
        for ( String token : a )
            if ( b.contains( token ) )
                intersection += 1;
        // calculate the union of x and y
        Set<String> set = new HashSet<String>();
        set.addAll( a );
        set.addAll( b );
        int union = set.size();
        // finally 
        return (double) intersection / union;
    }

}