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
package cc.clabs.stratosphere.fuzzyjoin.contracts.stage1;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import java.util.Iterator;

/**
 * Calculates the total frequency of every Token.
 * The frequency will be emitted as the key, in order
 * to sort the overall list by frequency while shipping.
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
public class CountTokens extends ReduceStub<PactString, PactInteger, PactInteger, PactString> {

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce( PactString key, Iterator<PactInteger> values, Collector<PactInteger, PactString> collector ) {
        int sum = 0;
        while ( values.hasNext() )
            sum += values.next().getValue();
        collector.collect( new PactInteger( sum ), key );
    }


}