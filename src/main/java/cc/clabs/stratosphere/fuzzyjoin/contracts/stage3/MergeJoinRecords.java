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

import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordAndDouble;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordAndRecord;
import cc.clabs.stratosphere.fuzzyjoin.types.PactRecordJoinKey;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.base.PactDouble;

/**
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class MergeJoinRecords extends MatchStub<PactRecordJoinKey, PactRecordAndDouble, PactRecordAndDouble, PactDouble, PactRecordAndRecord> {
    
    @Override
    public void match( PactRecordJoinKey joinkey, PactRecordAndDouble a, PactRecordAndDouble b, Collector<PactDouble, PactRecordAndRecord> collector ) {
        final PactDouble key = a.getSecond();
        final PactRecordAndRecord value = new PactRecordAndRecord( a.getFirst(), b.getFirst() );
        collector.collect( key, value );
    }

}