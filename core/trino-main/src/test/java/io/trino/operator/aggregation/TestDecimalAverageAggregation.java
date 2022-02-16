/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.trino.operator.aggregation.state.Int128State;
import io.trino.operator.aggregation.state.Int128StateFactory;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;

import static io.trino.operator.aggregation.DecimalAverageAggregation.average;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static java.math.BigInteger.ONE;
import static java.math.BigInteger.TEN;
import static java.math.BigInteger.ZERO;
import static java.math.RoundingMode.HALF_UP;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDecimalAverageAggregation
{
    private static final BigInteger TWO = new BigInteger("2");
    private static final BigInteger ONE_HUNDRED = new BigInteger("100");
    private static final BigInteger TWO_HUNDRED = new BigInteger("200");
    private static final DecimalType TYPE = createDecimalType(38, 0);

    private Int128State decimalState;
    private Int128State counterOverflowState;
    private AccumulatorStateFactory<Int128State> int128StateFactory;

    @BeforeMethod
    public void setUp()
    {
        int128StateFactory = new Int128StateFactory();
        decimalState = int128StateFactory.createSingleState();
        counterOverflowState = int128StateFactory.createSingleState();
    }

    @Test
    public void testOverflow()
    {
        addToState(decimalState, counterOverflowState, TWO.pow(126));

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 1);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 0);
        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(126)));

        addToState(decimalState, counterOverflowState, TWO.pow(126));

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 2);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 63, 0));
        assertAverageEquals(TWO.pow(126));
    }

    @Test
    public void testUnderflow()
    {
        addToState(decimalState, counterOverflowState, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 1);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 0);
        assertEquals(getDecimal(decimalState), Decimals.MIN_UNSCALED_DECIMAL);

        addToState(decimalState, counterOverflowState, Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 2);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(0x698966AF4AF2770BL, 0xECEBBB8000000002L));

        assertAverageEquals(Decimals.MIN_UNSCALED_DECIMAL.toBigInteger());
    }

    @Test
    public void testUnderflowAfterOverflow()
    {
        addToState(decimalState, counterOverflowState, TWO.pow(126));
        addToState(decimalState, counterOverflowState, TWO.pow(126));
        addToState(decimalState, counterOverflowState, TWO.pow(125));

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 1);
//        assertEquals(getDecimal(decimalState), Int128.valueOf((1L << 63) | (1L << 61), 0));

        addToState(decimalState, counterOverflowState, TWO.pow(126).negate());
        addToState(decimalState, counterOverflowState, TWO.pow(126).negate());
        addToState(decimalState, counterOverflowState, TWO.pow(126).negate());

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 0);
//        assertEquals(getDecimal(decimalState), Int128.valueOf(TWO.pow(125).negate()));

        assertAverageEquals(TWO.pow(125).negate().divide(BigInteger.valueOf(6)));
    }

    @Test
    public void testCombineOverflow()
    {
        addToState(decimalState, counterOverflowState, TWO.pow(126));
        addToState(decimalState, counterOverflowState, TWO.pow(126));

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        Int128State otherCounterOverflowState = int128StateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterOverflowState, TWO.pow(126));
        addToState(otherDecimalState, otherCounterOverflowState, TWO.pow(126));

        DecimalAverageAggregation.combine(counterOverflowState, decimalState, otherCounterOverflowState, otherDecimalState);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 4);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 1);
        assertEquals(getDecimal(decimalState), Int128.ZERO);

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .divide(BigInteger.valueOf(4));

        assertAverageEquals(expectedAverage);
    }

    @Test
    public void testCombineUnderflow()
    {
        addToState(decimalState, counterOverflowState, TWO.pow(125).negate());
        addToState(decimalState, counterOverflowState, TWO.pow(126).negate());

        Int128State otherDecimalState = int128StateFactory.createSingleState();
        Int128State otherCounterOverflowState = int128StateFactory.createSingleState();

        addToState(otherDecimalState, otherCounterOverflowState, TWO.pow(125).negate());
        addToState(otherDecimalState, otherCounterOverflowState, TWO.pow(126).negate());

        DecimalAverageAggregation.combine(counterOverflowState, decimalState, otherCounterOverflowState, otherDecimalState);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset()], 4);
        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], -1);
        assertEquals(getDecimal(decimalState), Int128.valueOf(1L << 62, 0));

        BigInteger expectedAverage = BigInteger.ZERO
                .add(TWO.pow(126))
                .add(TWO.pow(126))
                .add(TWO.pow(125))
                .add(TWO.pow(125))
                .negate()
                .divide(BigInteger.valueOf(4));

        assertAverageEquals(expectedAverage);
    }

    @Test(dataProvider = "testNoOverflowDataProvider")
    public void testNoOverflow(List<BigInteger> numbers)
    {
        testNoOverflow(createDecimalType(38, 0), numbers);
        testNoOverflow(createDecimalType(38, 2), numbers);
    }

    private void testNoOverflow(DecimalType type, List<BigInteger> numbers)
    {
        Int128State decimalState = int128StateFactory.createSingleState();
        Int128State counterOverflowState = int128StateFactory.createSingleState();

        for (BigInteger number : numbers) {
            addToState(type, decimalState, counterOverflowState, number);
        }

        assertEquals(counterOverflowState.getArray()[counterOverflowState.getArrayOffset() +1], 0);
        BigInteger sum = numbers.stream().reduce(BigInteger.ZERO, BigInteger::add);
        assertEquals(getDecimal(decimalState), Int128.valueOf(sum));

        BigDecimal expectedAverage = new BigDecimal(sum, type.getScale()).divide(BigDecimal.valueOf(numbers.size()), type.getScale(), HALF_UP);
        assertEquals(decodeBigDecimal(type, average(counterOverflowState, decimalState, type)), expectedAverage);
    }

    @DataProvider
    public static Object[][] testNoOverflowDataProvider()
    {
        return new Object[][] {
                {ImmutableList.of(TEN.pow(37), ZERO)},
                {ImmutableList.of(TEN.pow(37).negate(), ZERO)},
                {ImmutableList.of(TWO, ONE)},
                {ImmutableList.of(ZERO, ONE)},
                {ImmutableList.of(TWO.negate(), ONE.negate())},
                {ImmutableList.of(ONE.negate(), ZERO)},
                {ImmutableList.of(ONE.negate(), ZERO, ZERO)},
                {ImmutableList.of(TWO.negate(), ZERO, ZERO)},
                {ImmutableList.of(TWO.negate(), ZERO)},
                {ImmutableList.of(TWO_HUNDRED, ONE_HUNDRED)},
                {ImmutableList.of(ZERO, ONE_HUNDRED)},
                {ImmutableList.of(TWO_HUNDRED.negate(), ONE_HUNDRED.negate())},
                {ImmutableList.of(ONE_HUNDRED.negate(), ZERO)}
        };
    }

    private static BigDecimal decodeBigDecimal(DecimalType type, Int128 average)
    {
        BigInteger unscaledVal = average.toBigInteger();
        return new BigDecimal(unscaledVal, type.getScale(), new MathContext(type.getPrecision()));
    }

    private void assertAverageEquals(BigInteger expectedAverage)
    {
        assertAverageEquals(expectedAverage, TYPE);
    }

    private void assertAverageEquals(BigInteger expectedAverage, DecimalType type)
    {
        assertEquals(average(decimalState, counterOverflowState, type).toBigInteger(), expectedAverage);
    }

    private static void addToState(Int128State decimalState, Int128State counterOverflowState, BigInteger value)
    {
        addToState(TYPE, decimalState, counterOverflowState, value);
    }

    private static void addToState(DecimalType type, Int128State decimalState, Int128State counterOverflowState, BigInteger value)
    {
        BlockBuilder blockBuilder = type.createFixedSizeBlockBuilder(1);
        type.writeObject(blockBuilder, Int128.valueOf(value));
        if (type.isShort()) {
            DecimalAverageAggregation.inputShortDecimal(decimalState, counterOverflowState, blockBuilder.build(), 0);
        }
        else {
            DecimalAverageAggregation.inputLongDecimal(decimalState, counterOverflowState, blockBuilder.build(), 0);
        }
    }

    private Int128 getDecimal(Int128State state)
    {
        int offset = state.getArrayOffset();
        return Int128.valueOf(state.getArray()[offset], state.getArray()[offset + 1]);
    }
}
