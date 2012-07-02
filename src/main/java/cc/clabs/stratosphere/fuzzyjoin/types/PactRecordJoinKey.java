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

import eu.stratosphere.pact.common.type.base.PactPair;


/**
 * Subclass of a regular PactPair. HashCode has been overridden
 * to ensure that the Hash is independent from the order
 * of the pair.
 * 
 * @author Robert Pagel <rob at clabs.cc>
 */
public class PactRecordJoinKey extends PactPair<PactRecordKey, PactRecordKey> {

    public PactRecordJoinKey() { super(); }
    
    public PactRecordJoinKey( PactRecordKey first, PactRecordKey second ) {
        setFirst( first );
        setSecond( second );
    }
    
    @Override
    public int hashCode() {
        return 31 * (1 + getFirst().hashCode() + getSecond().hashCode());
    }

}