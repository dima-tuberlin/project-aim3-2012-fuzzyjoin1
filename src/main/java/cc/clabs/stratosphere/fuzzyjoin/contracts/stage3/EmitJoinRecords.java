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
package cc.clabs.stratosphere.fuzzyjoin.contracts.stage3;

import cc.clabs.stratosphere.fuzzyjoin.types.*;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class EmitJoinRecords extends MatchStub<PactRecordKey, PactRecord, PactRecordJoinKeysAndSimilarity, PactRecordJoinKey, PactRecordAndDouble> {
    
    @Override
    public void match( PactRecordKey joinkey, PactRecord record, PactRecordJoinKeysAndSimilarity pair, Collector<PactRecordJoinKey, PactRecordAndDouble> collector ) {
        final PactRecordJoinKey key = pair.getFirst();
        final PactRecordAndDouble value = new PactRecordAndDouble( record, pair.getSecond() );
        collector.collect( key, value );
    }

}