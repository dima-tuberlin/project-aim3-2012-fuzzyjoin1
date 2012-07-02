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
import eu.stratosphere.pact.common.contract.OutputContract.*;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.*;

/**
 * Generates distinct pairs of records that have at least one
 * token in common.
 *
 * @author Robert Pagel <rob at clabs.cc>
 */
public class JoinCandidatePairs extends CoGroupStub<PactString, PactRecord, PactRecord, PactRecordAndRecord, PactNull> {

    private final static PactNull nix = new PactNull();
    
    @Override
    public void coGroup( PactString prefix, Iterator<PactRecord> r, Iterator<PactRecord> s, Collector<PactRecordAndRecord, PactNull> collector ) {
        Set<PactRecord> listS = new HashSet<PactRecord>();
        while( s.hasNext() ) listS.add( s.next() );
        while( r.hasNext()  ) {
            PactRecord R = r.next();
            s = listS.iterator();
            while ( s.hasNext() ) {
                PactRecord S = s.next();
                collector.collect( new PactRecordAndRecord( R, S ), nix);
            }
            
        }
    }
 
}